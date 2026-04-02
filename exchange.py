import ccxt
import logging
import pandas as pd
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor

from config import BINANCE_API_KEY, BINANCE_API_SECRET, TESTNET

logger = logging.getLogger(__name__)


class BinanceFuturesClient:
    """ccxt üzerinden Binance USDM Futures bağlantısı."""

    def __init__(self):
        options = {
            "defaultType":       "future",
            "adjustForTimeDifference": True,
        }
        if TESTNET:
            options["urls"] = {
                "api": {
                    "public":  "https://testnet.binancefuture.com",
                    "private": "https://testnet.binancefuture.com",
                }
            }

        self.client = ccxt.binance({
            "apiKey":  BINANCE_API_KEY,
            "secret":  BINANCE_API_SECRET,
            "options": options,
        })

        # Market bilgilerini önbellekle (fiyat/miktar hassasiyeti için)
        self._load_markets()
        logger.info("Binance Futures bağlantısı kuruldu (Testnet: %s)", TESTNET)

    # ─── Market Bilgisi ───────────────────────────────────────────────────────

    def _load_markets(self) -> None:
        try:
            self.markets = self.client.load_markets()
        except Exception as exc:
            logger.error("Market bilgileri yüklenemedi: %s", exc)
            self.markets = {}

    def get_server_time_ms(self) -> int:
        """Binance sunucu zamanını milisaniye olarak döndürür."""
        return self.client.fetch_time()

    # ─── OHLCV ───────────────────────────────────────────────────────────────

    def get_ohlcv(
        self,
        symbol:           str,
        interval:         str = "15m",
        limit:            int = 300,
        convert_to_float: bool = True,
    ) -> Optional[pd.DataFrame]:
        """
        Tek sembol için OHLCV verisi çeker.
        Dönen DataFrame index=datetime (UTC), sütunlar: open high low close volume
        """
        try:
            raw = self.client.fetch_ohlcv(symbol, timeframe=interval, limit=limit)
            df  = pd.DataFrame(raw, columns=["time", "open", "high", "low", "close", "volume"])
            df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
            df.set_index("time", inplace=True)
            if convert_to_float:
                df = df.astype(float)
            return df
        except Exception as exc:
            logger.error("OHLCV çekme hatası (%s): %s", symbol, exc)
            return None

    def get_multiple_ohlcv(
        self,
        symbols:  List[str],
        interval: str = "15m",
        limit:    int = 500,
    ) -> Dict[str, Optional[pd.DataFrame]]:
        """Birden fazla sembol için veri paralel çeker."""
        with ThreadPoolExecutor(max_workers=len(symbols)) as executor:
            futures = {sym: executor.submit(self.get_ohlcv, sym, interval, limit) for sym in symbols}
            return {sym: fut.result() for sym, fut in futures.items()}

    # ─── Hesap & Kaldıraç ─────────────────────────────────────────────────────

    def set_leverage(self, symbol: str, leverage: int) -> bool:
        """Sembol bazlı kaldıraç ayarlar."""
        try:
            self.client.set_leverage(leverage, symbol)
            logger.info("%s kaldıraç ayarlandı: %sx", symbol, leverage)
            return True
        except ccxt.ExchangeError as exc:
            # Kaldıraç zaten aynıysa Binance hata fırlatır, görmezden gel
            if "No need to change leverage" in str(exc):
                logger.debug("%s kaldıraç zaten %sx", symbol, leverage)
                return True
            logger.warning("%s kaldıraç ayarlama hatası: %s", symbol, exc)
            return False

    def set_margin_type(self, symbol: str, margin_type: str = "ISOLATED") -> bool:
        """Margin tipini ayarlar: ISOLATED veya CROSSED."""
        try:
            self.client.set_margin_mode(margin_type.lower(), symbol)
            logger.info("%s margin tipi: %s", symbol, margin_type)
            return True
        except ccxt.ExchangeError as exc:
            if "No need to change margin type" in str(exc):
                return True
            logger.warning("%s margin tipi ayarlama hatası: %s", symbol, exc)
            return False

    def get_open_positions(self) -> List[Dict]:
        """
        Tüm açık pozisyonları döndürür.
        Sadece size > 0 olanlar döner.
        """
        try:
            positions = self.client.fetch_positions()
            return [p for p in positions if abs(float(p.get("contracts", 0) or 0)) > 0]
        except Exception as exc:
            logger.error("Pozisyonlar alınamadı: %s", exc)
            return []

    def get_position(self, symbol: str) -> Optional[Dict]:
        """Belirli bir sembolün açık pozisyonunu döndürür."""
        try:
            positions = self.client.fetch_positions([symbol])
            for pos in positions:
                if abs(float(pos.get("contracts", 0) or 0)) > 0:
                    return pos
            return None
        except Exception as exc:
            logger.error("%s pozisyon alınamadı: %s", symbol, exc)
            return None

    # ─── Emir İşlemleri ───────────────────────────────────────────────────────

    def place_market_order(
        self,
        symbol:      str,
        side:        str,   # "buy" | "sell"
        amount:      float,
        reduce_only: bool = False,
    ) -> Optional[Dict]:
        """Market emri gönderir."""
        try:
            params = {"reduceOnly": reduce_only}
            order  = self.client.create_order(
                symbol=symbol,
                type="market",
                side=side,
                amount=amount,
                params=params,
            )
            logger.info("Market emir gönderildi: %s %s %s adet", symbol, side.upper(), amount)
            return order
        except Exception as exc:
            logger.error("Market emir hatası (%s %s): %s", symbol, side, exc)
            return None

    def place_limit_order(
        self,
        symbol:      str,
        side:        str,
        amount:      float,
        price:       float,
        reduce_only: bool = False,
        time_in_force: str = "GTC",
    ) -> Optional[Dict]:
        """Limit emri gönderir."""
        try:
            params = {
                "reduceOnly":  reduce_only,
                "timeInForce": time_in_force,
            }
            order = self.client.create_order(
                symbol=symbol,
                type="limit",
                side=side,
                amount=amount,
                price=price,
                params=params,
            )
            logger.info("Limit emir gönderildi: %s %s %.5f @ %.5f", symbol, side.upper(), amount, price)
            return order
        except Exception as exc:
            logger.error("Limit emir hatası (%s %s): %s", symbol, side, exc)
            return None

    def place_stop_market_order(
        self,
        symbol:      str,
        side:        str,
        amount:      float,
        stop_price:  float,
        reduce_only: bool = True,
    ) -> Optional[Dict]:
        """Stop-Market (SL) emri gönderir."""
        try:
            params = {
                "stopPrice":  stop_price,
                "reduceOnly": reduce_only,
            }
            order = self.client.create_order(
                symbol=symbol,
                type="stop_market",
                side=side,
                amount=amount,
                params=params,
            )
            logger.info("Stop-Market emir gönderildi: %s %s stop=%.5f", symbol, side.upper(), stop_price)
            return order
        except Exception as exc:
            logger.error("Stop-Market emir hatası (%s %s): %s", symbol, side, exc)
            return None

    def cancel_order(self, symbol: str, order_id: str) -> bool:
        """Normal ve algo (stop-market) emirleri iptal eder."""
        try:
            # Önce normal emir olarak dene
            try:
                self.client.cancel_order(order_id, symbol)
                logger.info("Emir iptal edildi: %s / %s", symbol, order_id)
                return True
            except Exception:
                pass
    
            # Başarısız olursa algo emir olarak dene
            raw_symbol = symbol.replace("/", "").replace(":USDT", "")
            self.client.fapiPrivateDeleteAlgoOrder({
                "symbol": raw_symbol,
                "algoId": order_id,
            })
            logger.info("Algo emir iptal edildi: %s / %s", symbol, order_id)
            return True
    
        except ccxt.OrderNotFound:
            logger.debug("Emir zaten yok: %s / %s", symbol, order_id)
            return True
        except Exception as exc:
            logger.error("Emir iptal hatası (%s / %s): %s", symbol, order_id, exc)
            return False

    def get_order(self, symbol: str, order_id: str) -> Optional[Dict]:
        """Emir bilgilerini sorgular (açık veya geçmiş)."""
        try:
            return self.client.fetch_order(order_id, symbol)
        except ccxt.OrderNotFound:
            return None
        except Exception as exc:
            logger.error("Emir sorgulama hatası (%s / %s): %s", symbol, order_id, exc)
            return None

    def get_open_orders(self, symbol: str) -> List[Dict]:
        """Açık emirleri döndürür."""
        try:
            return self.client.fetch_open_orders(symbol)
        except Exception as exc:
            logger.error("Açık emirler alınamadı (%s): %s", symbol, exc)
            return []
