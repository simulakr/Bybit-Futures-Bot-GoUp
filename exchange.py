import os
import pandas as pd
from pybit.unified_trading import HTTP
from dotenv import load_dotenv
from typing import List, Optional, Dict
from concurrent.futures import ThreadPoolExecutor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
load_dotenv()

BYBIT_MAX_LIMIT = 200  # Bybit get_kline hard limit


class BybitFuturesAPI:
    def __init__(self, testnet: bool = False):
        self.session = HTTP(
            api_key=os.getenv('BYBIT_API_KEY'),
            api_secret=os.getenv('BYBIT_API_SECRET'),
            testnet=testnet,
        )
        # Cache: {symbol: DataFrame (1000 bar, index=UTC datetime)}
        self._cache: Dict[str, pd.DataFrame] = {}
        logger.info("Bybit Futures API bağlantısı başarılı (Testnet: %s)", testnet)

    # ─── Tekli OHLCV ──────────────────────────────────────────────────────────

    def get_ohlcv(
        self,
        symbol:           str = 'SOLUSLT',
        interval:         str = '15',
        limit:            int = 200,
        convert_to_float: bool = True,
    ) -> Optional[pd.DataFrame]:
        """
        Bybit'ten OHLCV verisi çeker (max 200 bar).
        Index: UTC datetime, sütunlar: open high low close volume
        """
        try:
            response = self.session.get_kline(
                category="linear",
                symbol=symbol,
                interval=interval,
                limit=limit,
            )
            if response['retCode'] != 0:
                raise Exception(response['retMsg'])

            klines = response['result']['list']
            df = pd.DataFrame(klines, columns=[
                'time', 'open', 'high', 'low', 'close', 'volume', 'turnover'
            ])
            df = df[['time', 'open', 'high', 'low', 'close', 'volume']].copy()
            df['time'] = pd.to_datetime(df['time'].astype(int), unit='ms', utc=True)
            if convert_to_float:
                df[['open', 'high', 'low', 'close', 'volume']] = \
                    df[['open', 'high', 'low', 'close', 'volume']].astype(float)
            df.set_index('time', inplace=True)
            df = df.iloc[::-1]  # Bybit ters sıra gönderir, eskiden yeniye çevir
            return df

        except Exception as e:
            logger.error("Veri çekme hatası (%s): %s", symbol, e)
            return None

    # ─── 1000 Bar Çekme ───────────────────────────────────────────────────────

    def fetch_1000_bars(self, symbol: str, interval: str = '15') -> Optional[pd.DataFrame]:
        """
        1000 bar çekmek için 5 adet 200'lük istek atar, birleştirir.
        Bybit'in hard limiti 200 olduğu için tekli istekle 1000 alınamaz.
        """
        try:
            all_dfs = []
            # İlk isteği at (en güncel 200 bar)
            df = self.get_ohlcv(symbol, interval, limit=BYBIT_MAX_LIMIT)
            if df is None or df.empty:
                return None
            all_dfs.append(df)

            # Geriye doğru 4 istek daha at
            for _ in range(4):
                oldest_time = all_dfs[-1].index[0]
                # Bybit'te end parametresi ms cinsinden
                end_ms = int(oldest_time.timestamp() * 1000) - 1

                response = self.session.get_kline(
                    category="linear",
                    symbol=symbol,
                    interval=interval,
                    limit=BYBIT_MAX_LIMIT,
                    end=end_ms,
                )
                if response['retCode'] != 0:
                    break

                klines = response['result']['list']
                if not klines:
                    break

                chunk = pd.DataFrame(klines, columns=[
                    'time', 'open', 'high', 'low', 'close', 'volume', 'turnover'
                ])
                chunk = chunk[['time', 'open', 'high', 'low', 'close', 'volume']].copy()
                chunk['time'] = pd.to_datetime(chunk['time'].astype(int), unit='ms', utc=True)
                chunk[['open', 'high', 'low', 'close', 'volume']] = \
                    chunk[['open', 'high', 'low', 'close', 'volume']].astype(float)
                chunk.set_index('time', inplace=True)
                chunk = chunk.iloc[::-1]
                all_dfs.append(chunk)

            # Birleştir, sırala, tekrarları at
            combined = pd.concat(all_dfs)
            combined = combined[~combined.index.duplicated(keep='last')]
            combined.sort_index(inplace=True)

            # Son 1000 barı al
            combined = combined.iloc[-1000:]
            logger.info("%s için %d bar yüklendi", symbol, len(combined))
            return combined

        except Exception as e:
            logger.error("%s 1000 bar çekme hatası: %s", symbol, e)
            return None

    # ─── Cache Başlatma ───────────────────────────────────────────────────────

    def initialize_cache(self, symbols: List[str], interval: str = '15') -> None:
        """
        Bot başlarken her sembol için 1000 bar çekip cache'e yükler.
        Paralel çalışır.
        """
        logger.info("Cache başlatılıyor: %s", symbols)
        with ThreadPoolExecutor(max_workers=len(symbols)) as executor:
            futures = {sym: executor.submit(self.fetch_1000_bars, sym, interval) for sym in symbols}
            for sym, fut in futures.items():
                df = fut.result()
                if df is not None:
                    self._cache[sym] = df
                    logger.info("%s cache hazır (%d bar)", sym, len(df))
                else:
                    logger.error("%s cache başlatılamadı", sym)

    # ─── Cache Güncelleme ─────────────────────────────────────────────────────

    def update_cache(self, symbol: str, interval: str = '15', fetch_last: int = 3) -> Optional[pd.DataFrame]:
        """
        Her mumda sadece son 3 bar çeker, cache'e ekler, en eskiyi atar.
        Cache'i güncel tutar ve güncel DataFrame döndürür.
        """
        try:
            new_bars = self.get_ohlcv(symbol, interval, limit=fetch_last)
            if new_bars is None or new_bars.empty:
                logger.warning("%s yeni bar çekilemedi, cache kullanılıyor", symbol)
                return self._cache.get(symbol)

            if symbol not in self._cache:
                logger.warning("%s cache yok, 1000 bar çekiliyor", symbol)
                df = self.fetch_1000_bars(symbol, interval)
                if df is not None:
                    self._cache[symbol] = df
                return self._cache.get(symbol)

            # Yeni barları cache'e ekle
            combined = pd.concat([self._cache[symbol], new_bars])
            combined = combined[~combined.index.duplicated(keep='last')]
            combined.sort_index(inplace=True)

            # 1000 bar sınırını koru
            combined = combined.iloc[-1000:]
            self._cache[symbol] = combined
            return combined

        except Exception as e:
            logger.error("%s cache güncelleme hatası: %s", symbol, e)
            return self._cache.get(symbol)

    # ─── Ana Veri Çekme (Cache'li) ────────────────────────────────────────────

    def get_multiple_ohlcv(
        self,
        symbols:  List[str],
        interval: str = '15',
    ) -> Dict[str, Optional[pd.DataFrame]]:
        """
        Her mumda tüm semboller için cache günceller, güncel DataFrame döndürür.
        Cache yoksa otomatik başlatır.
        """
        # Cache hiç başlatılmamışsa başlat
        missing = [s for s in symbols if s not in self._cache]
        if missing:
            self.initialize_cache(missing, interval)

        with ThreadPoolExecutor(max_workers=len(symbols)) as executor:
            futures = {sym: executor.submit(self.update_cache, sym, interval) for sym in symbols}
            return {sym: fut.result() for sym, fut in futures.items()}
