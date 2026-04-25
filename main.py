import time
import logging
import datetime
import pandas as pd
from typing import Dict, Optional
from config import SYMBOLS, INTERVAL, LEVERAGE
from exchange import BybitFuturesAPI
from indicators import calculate_indicators
from entry_strategies import check_long_entry, check_short_entry
from position_manager import PositionManager

# Log ayarları
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('trading_bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TradingBot:
    def __init__(self, testnet: bool = False):
        self.api = BybitFuturesAPI(testnet=testnet)
        self.position_manager = PositionManager(self.api.session)
        self.symbols = SYMBOLS
        self.interval = INTERVAL
        self._initialize_account()
        self.api.initialize_cache(self.symbols, self.interval)
        self._load_existing_positions()

    def _initialize_account(self):
        """ByBit için hesap ayarlarını yapılandır"""
        for symbol in self.symbols:
            try:
                self.api.session.set_leverage(
                category="linear",
                symbol=symbol,
                buyLeverage=str(LEVERAGE),
                sellLeverage=str(LEVERAGE)
                )
                logger.info(f"{symbol} kaldıraç ayarlandı: {LEVERAGE}x")
            except Exception as e:
                if "leverage not modified" in str(e):
                    logger.debug(f"{symbol} kaldıraç zaten {LEVERAGE}x olarak ayarlı")
                else:
                    logger.warning(f"{symbol} kaldıraç ayarlama uyarısı: {str(e)}")

    def _load_existing_positions(self):
        """Bybit'teki mevcut pozisyonları bot hafızasına yükle (TP/SL emirleri dahil)"""
        try:
            positions = self.api.session.get_positions(category='linear', settleCoin='USDT')
            if positions['retCode'] == 0:
                for pos in positions['result']['list']:
                    if float(pos.get('size', 0)) > 0:  # Açık pozisyon varsa
                        symbol = pos['symbol']
                        direction = 'LONG' if pos['side'] == 'Buy' else 'SHORT'
                        quantity = float(pos['size'])
                        
                        # Açık emirleri çek (TP/SL emirlerini bul)
                        oco_pair = self._find_tp_sl_orders(symbol, direction, quantity)
                        
                        # Bot hafızasına ekle
                        position_data = {
                            'symbol': symbol,
                            'direction': direction,
                            'entry_price': float(pos['avgPrice']),
                            'quantity': quantity,
                            'take_profit': float(pos['takeProfit']) if pos['takeProfit'] else None,
                            'stop_loss': float(pos['stopLoss']) if pos['stopLoss'] else None,
                            'order_id': None
                        }
                        
                        # OCO pair varsa ekle
                        if oco_pair:
                            position_data['oco_pair'] = oco_pair
                            logger.info(f"{symbol} pozisyon + TP/SL emirleri yüklendi: {direction}")
                        else:
                            logger.warning(f"{symbol} pozisyon yüklendi ama TP/SL emirleri bulunamadı")
                        
                        self.position_manager.active_positions[symbol] = position_data
                        
        except Exception as e:
            logger.error(f"Mevcut pozisyonlar yüklenirken hata: {e}")
    
    
    def _find_tp_sl_orders(self, symbol: str, direction: str, quantity: float) -> Optional[Dict]:
        """
        Belirli bir pozisyon için açık TP/SL emirlerini bulur ve OCO pair oluşturur
        """
        try:
            # Açık emirleri çek
            orders = self.api.session.get_open_orders(
                category='linear',
                symbol=symbol
            )
            
            if orders['retCode'] != 0:
                return None
            
            tp_order_id = None
            sl_order_id = None
            expected_side = "Sell" if direction == "LONG" else "Buy"
            
            # TP ve SL emirlerini bul
            for order in orders['result']['list']:
                if order['side'] != expected_side:
                    continue
                
                order_qty = float(order['qty'])
                # Miktar eşleşmesi (küçük farkları tolere et)
                if abs(order_qty - quantity) > quantity * 0.01:  # %1 tolerans
                    continue
                
                # Limit emir = TP
                if order['orderType'] == 'Limit' and order.get('reduceOnly'):
                    tp_order_id = order['orderId']
                
                # Stop/Market emir = SL
                elif order['orderType'] == 'Market' and order.get('triggerPrice'):
                    sl_order_id = order['orderId']
            
            # Her iki emir de bulunduysa OCO pair oluştur
            if tp_order_id and sl_order_id:
                return {
                    'symbol': symbol,
                    'tp_order_id': tp_order_id,
                    'sl_order_id': sl_order_id,
                    'active': True
                }
            else:
                logger.warning(f"{symbol} TP/SL emirleri eksik - TP: {tp_order_id}, SL: {sl_order_id}")
                return None
                
        except Exception as e:
            logger.error(f"{symbol} TP/SL emirleri aranırken hata: {e}")
            return None
        """
        Mevcut pozisyonun sadece TP/SL'sini günceller (Senaryo 2a)
        """
        try:
            position = self.active_positions[symbol]
            
            # Eski TP/SL emirlerini iptal et
            if 'oco_pair' in position:
                logger.info(f"{symbol} eski TP/SL emirleri iptal ediliyor...")
                self.exit_strategy.cancel_order(symbol, position['oco_pair']['tp_order_id'])
                self.exit_strategy.cancel_order(symbol, position['oco_pair']['sl_order_id'])
            
            # Yeni TP/SL seviyelerini hesapla
            tp_price, sl_price = self.exit_strategy.calculate_levels(entry_price, atr_value, direction, symbol)
            logger.info(f"{symbol} Yeni TP/SL hesaplandı | TP: {tp_price} | SL: {sl_price}")
            
            # Yeni limit TP/SL emirlerini gönder
            tp_sl_result = self.exit_strategy.set_limit_tp_sl(
                symbol=symbol,
                direction=direction,
                tp_price=tp_price,
                sl_price=sl_price,
                quantity=position['quantity']
            )
            
            if tp_sl_result.get('success'):
                # Pozisyon bilgilerini güncelle
                position['take_profit'] = tp_price
                position['stop_loss'] = sl_price
                position['current_pct_atr'] = pct_atr
                position['oco_pair'] = tp_sl_result['oco_pair']
                
                logger.info(f"{symbol} TP/SL başarıyla güncellendi")
                return position
            else:
                logger.error(f"{symbol} TP/SL güncellenemedi")
                return None
                
        except Exception as e:
            logger.error(f"{symbol} TP/SL güncelleme hatası: {str(e)}")
            return None
            
    def _is_weekend_trading_blocked(self) -> bool:
        """
        Türkiye saatiyle Cuma 23:59 - Pazar 23:59 arası işlem almayı engeller.
        Bu aralıkta True döner (işlem yasak), dışında False döner.
        """
        try:
            import pytz
            
            turkey_tz = pytz.timezone('Europe/Istanbul')
            
            # Bybit sunucu zamanını al, Türkiye saatine çevir
            server_time = self.api.session.get_server_time()
            ts = int(server_time['result']['timeSecond'])
            utc_time = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
            turkey_time = utc_time.astimezone(turkey_tz)
            
            weekday = turkey_time.weekday()  # 0=Pazartesi, 4=Cuma, 5=Cumartesi, 6=Pazar
            hour = turkey_time.hour
            minute = turkey_time.minute
            
            # Cuma 23:59'dan itibaren blokla
            if weekday == 4 and hour == 23 and minute >= 59:
                logger.info(f"Hafta sonu bloğu aktif: Cuma {turkey_time.strftime('%H:%M')} (TR)")
                return True
            
            # Tüm Cumartesi- Pazar
            if weekday == 5 or weekday == 6:
                logger.info(f"Hafta sonu bloğu aktif:  {turkey_time.strftime('%H:%M')} (TR)")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Hafta sonu kontrol hatası: {e}")
            return False  # Hata durumunda işleme izin ver
        
    def _wait_until_next_candle(self) -> None:
        """Bybit sunucu saatiyle 15 dakikalık mum kapanışını bekler. Hedef: XX:15:01"""
        try:
            server_time = self.api.session.get_server_time()
            ts          = int(server_time['result']['timeSecond'])
            current     = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
            minute      = current.minute

            for target in [15, 30, 45, 0]:
                if target == 0:
                    target_time = current.replace(minute=0, second=0, microsecond=300000) + datetime.timedelta(hours=1)
                    break
                if minute < target:
                    target_time = current.replace(minute=target, second=0, microsecond=300000)
                    break

            if target_time <= current:
                target_time += datetime.timedelta(hours=1)

            wait_seconds = (target_time - current).total_seconds()

            logger.info(
                f"Bekleniyor | Şu an: {current.strftime('%H:%M:%S')} | "
                f"Hedef: {target_time.strftime('%H:%M:%S')} | Süre: {wait_seconds:.1f}s"
            )

            if wait_seconds > 0:
                time.sleep(wait_seconds)
            else:
                time.sleep(1)

            logger.info("Yeni mum başladı — veriler çekiliyor")

        except Exception as e:
            logger.error(f"Zamanlama hatası: {e}")
            time.sleep(60)

    # ─── Veri & Sinyal ────────────────────────────────────────────────────────

    def _get_market_data_batch(self) -> Dict[str, Optional[Dict]]:
        """Tüm semboller için OHLCV + indikatör hesaplar. Kapanmamış mumu atar."""
        all_data = self.api.get_multiple_ohlcv(self.symbols, self.interval)
        now      = pd.Timestamp.utcnow()
        results  = {}

        for symbol, df in all_data.items():
            if df is not None and not df.empty:
                try:
                    df = df[df.index < now]  # kapanmamış mumu at
                    if df.empty:
                        logger.warning(f"{symbol} filtre sonrası veri kalmadı")
                        results[symbol] = None
                        continue
                    df = calculate_indicators(df, symbol)
                    results[symbol] = df.iloc[-1].to_dict()
                except Exception as e:
                    logger.error(f"{symbol} indikatör hatası: {e}")
                    results[symbol] = None
            else:
                results[symbol] = None

        return results

    def _generate_signals(self, all_data: Dict[str, Optional[Dict]]) -> Dict[str, Optional[str]]:
        """Toplu veriden sinyal oluştur"""
        signals = {}
        for symbol, data in all_data.items():
            if not data:
                signals[symbol] = None
                continue

            if check_long_entry(data, symbol):
                signals[symbol] = 'LONG'
            elif check_short_entry(data, symbol):
                signals[symbol] = 'SHORT'
            else:
                signals[symbol] = None
        return signals

    def _execute_trades(self, signals: Dict[str, Optional[str]], all_data: Dict[str, Optional[Dict]]):
        """
        Sinyallere göre işlem aç
        NOT: TP/SL güncellemesi artık manage_positions() içinde yapılıyor
        """
        for symbol, signal in signals.items():
            if not signal or not all_data.get(symbol):
                continue
    
            data = all_data[symbol]
            
            # Yeni pozisyon veya ters sinyal durumunda open_position çağır
            # open_position içinde zaten tüm senaryolar yönetiliyor:
            # - Pozisyon yoksa: Yeni açar (Senaryo 1)
            # - Aynı yön: TP/SL günceller (Senaryo 2a)
            # - Ters yön: Eski kapatır, yeni açar (Senaryo 2b)
            
            self.position_manager.open_position(
                symbol=symbol,
                direction=signal,
                entry_price=data['close'],
                atr_value=data['z'],
                pct_atr=data['pct_z']
            )
    
    
    def run(self):
        """Ana çalıştırma döngüsü"""
        
        logger.info(f"Bot başlatıldı | Semboller: {self.symbols} | Zaman Aralığı: {self.interval}m")
        
        while True:
            try:
                # 15 dakika senkronizasyonu
                self._wait_until_next_candle()

                # --- HAFTA SONU KONTROLÜ ---
                if self._is_weekend_trading_blocked():
                    logger.info("Hafta sonu modu: İşlem atlanıyor, sonraki muma geçiliyor.")
                    continue
                # ---------------------------
                
                start_time = time.time()
                
                # Toplu veri çekme ve işleme
                all_data = self._get_market_data_batch()
                signals = self._generate_signals(all_data)
                
                # 1. Pozisyon yönetimi (OCO kontrol + TP/SL güncelleme)
                self.position_manager.manage_positions(signals, all_data)
                
                # 2. Yeni pozisyonlar veya pozisyon güncellemeleri
                self._execute_trades(signals, all_data)
                
                elapsed = time.time() - start_time
                server_time_response = self.api.session.get_server_time()
                timestamp = int(server_time_response['result']['timeSecond'])
                server_time = datetime.datetime.fromtimestamp(timestamp).strftime("%H:%M:%S.%f")[:-4]
                
                logger.info(f"İşlem turu tamamlandı | Süre: {elapsed:.2f}s | Tamamlanma Saati: {server_time}")
                
            except KeyboardInterrupt:
                logger.info("Bot manuel olarak durduruldu")
                break
            except Exception as e:
                logger.error(f"Beklenmeyen hata: {str(e)}", exc_info=True)
                time.sleep(60)

if __name__ == "__main__":
    bot = TradingBot(testnet=False)
    bot.run()
