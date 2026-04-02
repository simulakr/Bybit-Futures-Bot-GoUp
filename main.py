import logging
import time
import datetime
from typing import Dict, Optional

from config import SYMBOLS, INTERVAL, SYMBOL_SETTINGS, DEFAULT_LEVERAGE
from exchange import BinanceFuturesClient
from indicators import calculate_indicators
from entry_strategies import check_long_entry, check_short_entry
from position_manager import PositionManager

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler("trading_bot.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


class TradingBot:
    def __init__(self):
        self.client           = BinanceFuturesClient()
        self.position_manager = PositionManager(self.client)
        self.symbols          = SYMBOLS
        self.interval         = INTERVAL

        self._initialize_account()
        self.position_manager.load_existing_positions()

    # ─── Hesap Kurulumu ───────────────────────────────────────────────────────

    def _initialize_account(self) -> None:
        """Her sembol için kaldıraç ayarlar."""
        for symbol in self.symbols:
            leverage = SYMBOL_SETTINGS.get(symbol, {}).get("leverage", DEFAULT_LEVERAGE)
            self.client.set_leverage(symbol, leverage)

    # ─── Zamanlama ────────────────────────────────────────────────────────────

    def _is_trading_hours(self) -> bool:
        """
        GMT+3'e göre Cuma 23:59 - Pazartesi 00:00 arası False döner.
        """
        server_ms = self.client.get_server_time_ms()
        now = datetime.datetime.fromtimestamp(
            server_ms / 1000,
            tz=datetime.timezone(datetime.timedelta(hours=3))
        )
        weekday = now.weekday()  # 0=Pazartesi, 4=Cuma, 5=Cumartesi, 6=Pazar
        
        # Cumartesi tüm gün
        if weekday == 5:
            return False
        # Pazar 00:00'dan önce (yani Pazartesi 00:00'a kadar)
        if weekday == 6:
            return False
        # Cuma 23:59 ve sonrası
        if weekday == 4 and now.hour == 23 and now.minute >= 59:
            return False
        
        return True
    
    def _wait_until_next_candle(self) -> None:
        """
        Binance sunucu saatiyle 15 dakikalık mum kapanışını bekler.
        Hedef: XX:14:59, XX:29:59, XX:44:59, XX:59:59
        """
        try:
            server_ms   = self.client.get_server_time_ms()
            current     = datetime.datetime.fromtimestamp(server_ms / 1000, tz=datetime.timezone.utc)
            minute      = current.minute

            # Bir sonraki çeyrek dakikayı bul
            for target in [14, 29, 44, 59]:
                if minute < target:
                    target_minute = target
                    break
            else:
                target_minute = 14  # Bir sonraki saatin ilk çeyreği

            target_time = current.replace(minute=target_minute, second=59, microsecond=0)
            if target_time <= current:
                target_time += datetime.timedelta(hours=1)

            wait_seconds = (target_time - current).total_seconds()

            logger.info(
                "Bekleniyor | Şu an: %s | Hedef: %s | Süre: %.1fs",
                current.strftime("%H:%M:%S"),
                target_time.strftime("%H:%M:%S"),
                wait_seconds,
            )

            if wait_seconds > 0:
                time.sleep(wait_seconds)
            else:
                time.sleep(1)

            logger.info("Yeni mum başladı — veriler çekiliyor")

        except Exception as exc:
            logger.error("Zamanlama hatası: %s", exc)
            time.sleep(60)

    # ─── Veri & Sinyal ────────────────────────────────────────────────────────

    def _fetch_market_data(self) -> Dict[str, Optional[Dict]]:
        """Tüm semboller için OHLCV + indikatör hesaplar."""
        all_ohlcv = self.client.get_multiple_ohlcv(self.symbols, self.interval)
        results   = {}

        for symbol, df in all_ohlcv.items():
            if df is None or df.empty:
                results[symbol] = None
                continue
            try:
                df = calculate_indicators(df, symbol)
                results[symbol] = df.iloc[-1].to_dict()
            except Exception as exc:
                logger.error("%s indikatör hesaplama hatası: %s", symbol, exc)
                results[symbol] = None

        return results

    def _generate_signals(self, all_data):
        signals = {}
        for symbol, data in all_data.items():
            if not data:
                signals[symbol] = None
            elif check_long_entry(data, symbol):
                signals[symbol] = "LONG"
                logger.info("%s LONG sinyali | breakout_2x: %s | goup_breakout_2x: %s",
                    symbol, data.get('pivot_go_breakout_2x'), data.get('pivot_goup_breakout_2x'))
            elif check_short_entry(data, symbol):
                signals[symbol] = "SHORT"
                logger.info("%s SHORT sinyali | breakdown_2x: %s | goup_breakdown_2x: %s",
                    symbol, data.get('pivot_go_breakdown_2x'), data.get('pivot_goup_breakdown_2x'))
            else:
                signals[symbol] = None
        return signals

    # ─── Emir Yürütme ─────────────────────────────────────────────────────────

    def _execute_trades(
        self,
        signals:  Dict[str, Optional[str]],
        all_data: Dict[str, Optional[Dict]],
    ) -> None:
        """Sinyallere göre pozisyon açar (PositionManager senaryoları yönetir)."""
        for symbol, signal in signals.items():
            if not signal or not all_data.get(symbol):
                continue

            data = all_data[symbol]
            self.position_manager.open_position(
                symbol=symbol,
                direction=signal,
                entry_price=data["close"],
                atr_value=data["z"],
                pct_atr=data["pct_z"],
            )

    # ─── Ana Döngü ────────────────────────────────────────────────────────────

    def run(self) -> None:
        logger.info("Bot başlatıldı | Semboller: %s | Aralık: %s", self.symbols, self.interval)

        while True:
            try:
                if not self._is_trading_hours():
                    now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=3)))
                    logger.info("İşlem saati dışı (%s) — 15 dk bekleniyor", now.strftime("%A %H:%M"))
                    time.sleep(900)
                    continue
                    
                self._wait_until_next_candle()
                start = time.time()

                all_data = self._fetch_market_data()
                signals  = self._generate_signals(all_data)

                # 1. Mevcut pozisyonları yönet (OCO kontrolü + TP/SL güncelleme)
                self.position_manager.manage_positions(signals, all_data)

                # 2. Yeni pozisyonları aç / ters pozisyonları tersine çevir
                self._execute_trades(signals, all_data)

                elapsed     = time.time() - start
                server_ms   = self.client.get_server_time_ms()
                server_time = datetime.datetime.fromtimestamp(server_ms / 1000, tz=datetime.timezone.utc)

                logger.info(
                    "Tur tamamlandı | Süre: %.2fs | Saat: %s",
                    elapsed,
                    server_time.strftime("%H:%M:%S"),
                )

            except KeyboardInterrupt:
                logger.info("Bot manuel olarak durduruldu")
                break
            except Exception as exc:
                logger.error("Beklenmeyen hata: %s", exc, exc_info=True)
                time.sleep(60)


if __name__ == "__main__":
    bot = TradingBot()
    bot.run()
