import logging
from typing import Dict, Optional, Tuple

from exchange import BinanceFuturesClient
from config import TP_ATR_MULTIPLIER, SL_ATR_MULTIPLIER, PRICE_PRECISION

logger = logging.getLogger(__name__)


class ExitStrategy:
    """TP/SL emir yönetimi — Binance USDM Futures (ccxt)."""

    def __init__(self, client: BinanceFuturesClient):
        self.client = client

    # ─── Seviye Hesaplama ─────────────────────────────────────────────────────

    def calculate_levels(
        self,
        entry_price: float,
        atr_value:   float,
        direction:   str,
        symbol:      str,
    ) -> Tuple[float, float]:
        """ATR bazlı TP ve SL fiyatlarını hesaplar."""
        precision = PRICE_PRECISION.get(symbol, 4)

        if direction == "LONG":
            tp = entry_price + TP_ATR_MULTIPLIER * atr_value
            sl = entry_price - SL_ATR_MULTIPLIER * atr_value
        else:
            tp = entry_price - TP_ATR_MULTIPLIER * atr_value
            sl = entry_price + SL_ATR_MULTIPLIER * atr_value

        return round(tp, precision), round(sl, precision)

    # ─── TP/SL Emir Yerleştirme ───────────────────────────────────────────────

    def place_tp_sl_orders(
        self,
        symbol:    str,
        direction: str,
        tp_price:  float,
        sl_price:  float,
        quantity:  float,
    ) -> Dict:
        """
        Limit TP + Stop-Market SL emirlerini yerleştirir.
        OCO takibi için her iki emir ID'sini döndürür.
        """
        close_side = "sell" if direction == "LONG" else "buy"

        # TP → Limit emri
        tp_order = self.client.place_limit_order(
            symbol=symbol,
            side=close_side,
            amount=quantity,
            price=tp_price,
            reduce_only=True,
        )

        if tp_order is None:
            logger.error("%s TP emri gönderilemedi", symbol)
            return {"success": False}

        # SL → Stop-Market emri
        sl_order = self.client.place_stop_market_order(
            symbol=symbol,
            side=close_side,
            amount=quantity,
            stop_price=sl_price,
            reduce_only=True,
        )

        if sl_order is None:
            logger.error("%s SL emri gönderilemedi — TP iptal ediliyor", symbol)
            self.client.cancel_order(symbol, tp_order["id"])
            return {"success": False}

        tp_id = tp_order["id"]
        sl_id = sl_order["id"]
        logger.info("%s TP Limit: %.5f (ID: %s)", symbol, tp_price, tp_id)
        logger.info("%s SL Stop:  %.5f (ID: %s)", symbol, sl_price, sl_id)

        oco_pair = {
            "symbol":     symbol,
            "tp_order_id": tp_id,
            "sl_order_id": sl_id,
            "active":     True,
        }
        return {"success": True, "oco_pair": oco_pair}

    # ─── OCO Kontrolü ─────────────────────────────────────────────────────────

    def check_and_cancel_oco(self, oco_pair: Dict) -> Dict:
        """
        TP veya SL tetiklendiyse karşı emri iptal eder.
        Dönen dict: triggered ('TP'|'SL'|None), cancelled ('SL'|'TP'|None)
        """
        if not oco_pair.get("active"):
            return {"already_handled": True}

        symbol = oco_pair["symbol"]
        tp_id  = oco_pair["tp_order_id"]
        sl_id  = oco_pair["sl_order_id"]

        tp_status = self._get_order_status(symbol, tp_id)
        sl_status = self._get_order_status(symbol, sl_id)

        if tp_status == "closed":
            logger.info("%s TP tetiklendi — SL iptal ediliyor", symbol)
            self.client.cancel_order(symbol, sl_id)
            oco_pair["active"] = False
            return {"triggered": "TP", "cancelled": "SL"}

        if sl_status == "closed":
            logger.info("%s SL tetiklendi — TP iptal ediliyor", symbol)
            self.client.cancel_order(symbol, tp_id)
            oco_pair["active"] = False
            return {"triggered": "SL", "cancelled": "TP"}

        return {"triggered": None}

    # ─── Yardımcı ─────────────────────────────────────────────────────────────

    def _get_order_status(self, symbol: str, order_id: str) -> str:
        """
        Önce normal emirlerde, sonra algo (stop-market) emirlerde arar.
        Döner: 'open' | 'closed' | 'canceled' | 'not_found'
        """
        # 1. Açık normal emirlerde ara
        open_orders = self.client.get_open_orders(symbol)
        for o in open_orders:
            if str(o["id"]) == str(order_id):
                return o.get("status", "open")
    
        # 2. Normal emir geçmişinde ara
        try:
            order = self.client.get_order(symbol, order_id)
            if order:
                return order.get("status", "unknown")
        except Exception:
            pass
    
        # 3. Algo (stop-market) emirlerde ara
        try:
            raw_symbol = symbol.replace("/", "").replace(":USDT", "")
            algo_orders = self.client.client.fapiPrivateGetAllAlgoOrders({
                "symbol": raw_symbol,
            })
            mapping = {
                        "NEW":      "open",
                        "WORKING":  "open",
                        "FILLED":   "closed",
                        "FINISHED": "closed",  # ← bunu ekle
                        "CANCELED": "canceled",
                        "EXPIRED":  "expired",
                    }
            for o in algo_orders:
                if str(o["algoId"]) == str(order_id):
                    status = o.get("algoStatus", "unknown")
                    return mapping.get(status, status.lower())
        except Exception:
            pass
    
        return "not_found"

    def cancel_tp_sl_orders(self, symbol: str, oco_pair: Dict) -> None:
        """OCO çiftinin her iki emrini de iptal eder."""
        self.client.cancel_order(symbol, oco_pair["tp_order_id"])
        self.client.cancel_order(symbol, oco_pair["sl_order_id"])
