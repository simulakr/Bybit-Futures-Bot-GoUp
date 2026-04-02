import logging
import time
from typing import Dict, Optional

from exchange import BinanceFuturesClient
from exit_strategies import ExitStrategy
from config import DEFAULT_RISK_USDT, DEFAULT_LEVERAGE, SYMBOL_SETTINGS, QUANTITY_PRECISION, SL_ATR_MULTIPLIER

logger = logging.getLogger(__name__)

# SL çarpanı (pozisyon büyüklüğü hesabında payda)
_SL_MULT = SL_ATR_MULTIPLIER


class PositionManager:
    """
    Pozisyon yaşam döngüsünü yönetir:
      - Yeni pozisyon açma
      - TP/SL güncelleme (aynı yön sinyali)
      - Tersine çevirme (ters yön sinyali)
      - OCO takibi
    """

    def __init__(self, client: BinanceFuturesClient):
        self.client        = client
        self.exit_strategy = ExitStrategy(client)
        self.active_positions: Dict[str, Dict] = {}

    # ─── Ana Giriş Noktası ────────────────────────────────────────────────────

    def open_position(
        self,
        symbol:      str,
        direction:   str,
        entry_price: float,
        atr_value:   float,
        pct_atr:     float,
    ) -> Optional[Dict]:
        """
        Senaryo 1  → Pozisyon yok: yeni aç
        Senaryo 2a → Aynı yön: TP/SL güncelle
        Senaryo 2b → Ters yön: kapat + yeni aç
        """
        if symbol in self.active_positions:
            existing = self.active_positions[symbol]["direction"]
            if existing == direction:
                logger.info("%s zaten %s yönünde — TP/SL güncelleniyor", symbol, direction)
                return self._update_tp_sl(symbol, direction, entry_price, atr_value, pct_atr)
            else:
                logger.info("%s ters sinyal (%s → %s) — pozisyon kapatılıp yeni açılıyor", symbol, existing, direction)
                self.close_position(symbol, reason="REVERSE_SIGNAL")

        return self._open_new_position(symbol, direction, entry_price, atr_value, pct_atr)

    # ─── Yeni Pozisyon ────────────────────────────────────────────────────────

    def _open_new_position(
        self,
        symbol:      str,
        direction:   str,
        entry_price: float,
        atr_value:   float,
        pct_atr:     float,
    ) -> Optional[Dict]:
        quantity = self._calculate_quantity(symbol, atr_value)
        side     = "buy" if direction == "LONG" else "sell"

        order = self.client.place_market_order(symbol=symbol, side=side, amount=float(quantity))
        if order is None:
            logger.error("%s market emri başarısız", symbol)
            return None

        # Pozisyon exchange'e yansıyana kadar bekle
        if not self._wait_for_position(symbol, direction, float(quantity)):
            logger.warning("%s pozisyon doğrulanamadı — TP/SL ayarlanamayacak", symbol)
            return None

        tp_price, sl_price = self.exit_strategy.calculate_levels(entry_price, atr_value, direction, symbol)
        logger.info("%s TP: %.5f | SL: %.5f", symbol, tp_price, sl_price)

        result = self.exit_strategy.place_tp_sl_orders(
            symbol=symbol,
            direction=direction,
            tp_price=tp_price,
            sl_price=sl_price,
            quantity=float(quantity),
        )

        if not result.get("success"):
            logger.warning("%s TP/SL ayarlanamadı — pozisyon kapatılıyor", symbol)
            self._emergency_close(symbol, direction, float(quantity))
            return None

        position = {
            "symbol":      symbol,
            "direction":   direction,
            "entry_price": entry_price,
            "quantity":    quantity,
            "take_profit": tp_price,
            "stop_loss":   sl_price,
            "pct_atr":     pct_atr,
            "order_id":    order["id"],
            "oco_pair":    result["oco_pair"],
        }
        self.active_positions[symbol] = position
        logger.info("%s pozisyon açıldı | %s | Miktar: %s | Entry: %.5f", symbol, direction, quantity, entry_price)
        return position

    # ─── TP/SL Güncelleme ─────────────────────────────────────────────────────

    def _update_tp_sl(
        self,
        symbol:      str,
        direction:   str,
        entry_price: float,
        atr_value:   float,
        pct_atr:     float,
    ) -> Optional[Dict]:
        position = self.active_positions[symbol]

        # Eski emirleri iptal et
        if "oco_pair" in position:
            self.exit_strategy.cancel_tp_sl_orders(symbol, position["oco_pair"])

        tp_price, sl_price = self.exit_strategy.calculate_levels(entry_price, atr_value, direction, symbol)
        logger.info("%s yeni TP: %.5f | SL: %.5f", symbol, tp_price, sl_price)

        result = self.exit_strategy.place_tp_sl_orders(
            symbol=symbol,
            direction=direction,
            tp_price=tp_price,
            sl_price=sl_price,
            quantity=float(position["quantity"]),
        )

        if result.get("success"):
            position.update({
                "take_profit": tp_price,
                "stop_loss":   sl_price,
                "pct_atr":     pct_atr,
                "oco_pair":    result["oco_pair"],
            })
            logger.info("%s TP/SL güncellendi", symbol)
            return position

        logger.error("%s TP/SL güncellenemedi", symbol)
        return None

    # ─── Pozisyon Kapatma ─────────────────────────────────────────────────────

    def close_position(self, symbol: str, reason: str = "MANUAL") -> bool:
        if symbol not in self.active_positions:
            logger.warning("%s kapatılacak pozisyon bulunamadı", symbol)
            return False

        position = self.active_positions[symbol]

        # OCO emirlerini iptal et
        if "oco_pair" in position:
            try:
                self.exit_strategy.cancel_tp_sl_orders(symbol, position["oco_pair"])
            except Exception as exc:
                logger.warning("%s TP/SL iptal hatası (zaten tetiklenmiş olabilir): %s", symbol, exc)

        side  = "sell" if position["direction"] == "LONG" else "buy"
        order = self.client.place_market_order(
            symbol=symbol,
            side=side,
            amount=float(position["quantity"]),
            reduce_only=True,
        )

        if order:
            logger.info("%s pozisyon kapatıldı | Sebep: %s", symbol, reason)
            del self.active_positions[symbol]
            return True

        logger.error("%s pozisyon kapatılamadı", symbol)
        return False

    # ─── Pozisyon Yönetim Döngüsü ─────────────────────────────────────────────

    def manage_positions(
        self,
        signals:  Dict[str, Optional[str]],
        all_data: Dict[str, Optional[Dict]],
    ) -> None:
        """
        Her mum sonunda çalışır:
        1. OCO kontrolü (TP/SL tetiklenme)
        2. Ters sinyal → kapat (yeni açılış _execute_trades'de yapılır)
        3. Aynı yön sinyali → TP/SL güncelle
        """
        # 1. OCO kontrolü
        self._monitor_oco_orders()

        # 2-3. Sinyal bazlı kontroller
        for symbol, position in list(self.active_positions.items()):
            signal    = signals.get(symbol)
            data      = all_data.get(symbol)
            direction = position["direction"]

            if not signal:
                continue

            if signal != direction:
                # Ters sinyal: sadece kapat, yeni pozisyon main loop'ta açılacak
                logger.info("%s ters sinyal (%s → %s)", symbol, direction, signal)
                continue

            # Aynı yön sinyali: TP/SL güncelle
            if data:
                logger.info("%s aynı yön sinyali — TP/SL güncelleniyor", symbol)
                tp_new, sl_new = self.exit_strategy.calculate_levels(
                    data["close"], data["z"], direction, symbol
                )
                if "oco_pair" in position:
                    self.exit_strategy.cancel_tp_sl_orders(symbol, position["oco_pair"])

                result = self.exit_strategy.place_tp_sl_orders(
                    symbol=symbol,
                    direction=direction,
                    tp_price=tp_new,
                    sl_price=sl_new,
                    quantity=float(position["quantity"]),
                )
                if result.get("success"):
                    position.update({
                        "entry_price": data["close"],
                        "take_profit": tp_new,
                        "stop_loss":   sl_new,
                        "oco_pair":    result["oco_pair"],
                    })
                    logger.info("%s TP/SL güncellendi | TP: %.5f | SL: %.5f", symbol, tp_new, sl_new)

    def _monitor_oco_orders(self) -> None:
        """TP/SL tetiklenip tetiklenmediğini kontrol eder; tetiklendiyse pozisyonu siler."""
        for symbol, position in list(self.active_positions.items()):
            if "oco_pair" not in position or not position["oco_pair"].get("active"):
                continue

            result = self.exit_strategy.check_and_cancel_oco(position["oco_pair"])

            if result.get("triggered"):
                logger.info("%s %s tetiklendi — pozisyon kapatıldı", symbol, result["triggered"])
                del self.active_positions[symbol]

    # ─── Başlangıçta Mevcut Pozisyonları Yükleme ──────────────────────────────

    def load_existing_positions(self) -> None:
        """
        Bot başlatıldığında exchange'deki açık pozisyonları hafızaya yükler.
        Mevcut TP/SL emirleri de OCO pair olarak eşlenir.
        """
        positions = self.client.get_open_positions()
        for pos in positions:
            symbol = pos["symbol"].replace("/", "").replace(":USDT", "")
            contracts = float(pos.get("contracts", 0) or 0)
            if contracts == 0:
                continue

            side      = pos.get("side", "")
            direction = "LONG" if side == "long" else "SHORT"
            entry     = float(pos.get("entryPrice", 0) or 0)

            oco_pair = self._find_tp_sl_orders(symbol, direction, contracts)

            self.active_positions[symbol] = {
                "symbol":      symbol,
                "direction":   direction,
                "entry_price": entry,
                "quantity":    contracts,
                "take_profit": None,
                "stop_loss":   None,
                "pct_atr":     None,
                "order_id":    None,
                "oco_pair":    oco_pair,
            }

            if oco_pair:
                logger.info("%s pozisyon + TP/SL yüklendi (%s)", symbol, direction)
            else:
                logger.warning("%s pozisyon yüklendi ama TP/SL emirleri bulunamadı", symbol)

    def _find_tp_sl_orders(self, symbol: str, direction: str, quantity: float) -> Optional[Dict]:
        close_side = "sell" if direction == "LONG" else "buy"
        raw_symbol = symbol.replace("/", "").replace(":USDT", "")
    
        tp_id = sl_id = None
    
        # TP → normal açık emirlerde ara (limit)
        orders = self.client.get_open_orders(symbol)
        for order in orders:
            if order.get("side") != close_side:
                continue
            amt = float(order.get("amount", 0) or 0)
            if abs(amt - quantity) > quantity * 0.01:
                continue
            if order.get("type") == "limit":
                tp_id = order["id"]
    
        # SL → algo emirlerde ara (stop_market)
        try:
            algo_orders = self.client.client.fapiPrivateGetAllAlgoOrders({
                "symbol": raw_symbol,
            })
            for o in algo_orders:
                if o.get("algoStatus") != "NEW":
                    continue
                if o.get("side", "").lower() != close_side:
                    continue
                if abs(float(o.get("quantity", 0)) - quantity) > quantity * 0.01:
                    continue
                sl_id = o["algoId"]
        except Exception as exc:
            logger.error("%s algo emirler alınamadı: %s", symbol, exc)
    
        if tp_id and sl_id:
            return {"symbol": symbol, "tp_order_id": tp_id, "sl_order_id": sl_id, "active": True}
    
        logger.warning("%s TP/SL emirleri eksik — TP: %s | SL: %s", symbol, tp_id, sl_id)
        return None

    # ─── Yardımcılar ──────────────────────────────────────────────────────────

    def _calculate_quantity(self, symbol: str, atr_value: float) -> str:
        cfg       = SYMBOL_SETTINGS.get(symbol, {})
        risk      = cfg.get("risk", DEFAULT_RISK_USDT)
        precision = QUANTITY_PRECISION.get(symbol, 2)
        raw_qty   = risk / (_SL_MULT * atr_value)
        qty       = round(raw_qty, precision)
        logger.info("%s pozisyon büyüklüğü: %s (risk=$%s)", symbol, qty, risk)
        return str(qty)

    def _wait_for_position(self, symbol: str, direction: str, expected_qty: float, timeout: float = 5.0) -> bool:
        """
        Pozisyonun exchange'e yansımasını bekler.
        Maksimum `timeout` saniye, 0.5s aralıklarla kontrol eder.
        """
        expected_side = "long" if direction == "LONG" else "short"
        attempts      = int(timeout / 0.5)

        for attempt in range(attempts):
            pos = self.client.get_position(symbol)
            if pos:
                contracts = abs(float(pos.get("contracts", 0) or 0))
                side      = pos.get("side", "")
                if side == expected_side and abs(contracts - expected_qty) < expected_qty * 0.05:
                    logger.info("%s pozisyon doğrulandı (deneme %d/%d)", symbol, attempt + 1, attempts)
                    return True
            time.sleep(0.5)

        logger.error("%s pozisyon %.1fs içinde doğrulanamadı", symbol, timeout)
        return False

    def _emergency_close(self, symbol: str, direction: str, quantity: float) -> None:
        """TP/SL ayarlanamadığında pozisyonu acil kapatır."""
        side  = "sell" if direction == "LONG" else "buy"
        self.client.place_market_order(symbol=symbol, side=side, amount=quantity, reduce_only=True)
        logger.warning("%s acil kapatma yapıldı", symbol)

    # ─── Sorgular ─────────────────────────────────────────────────────────────

    def get_position(self, symbol: str) -> Optional[Dict]:
        return self.active_positions.get(symbol)

    def has_position(self, symbol: str) -> bool:
        return symbol in self.active_positions
