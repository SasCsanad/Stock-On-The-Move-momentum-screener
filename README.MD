# Momentum Részvény Szűrési Szkript

## Áttekintés
Ez a szkript az S&P 500 részvényeit elemzi és szűri momentum indikátorok és trendanalízis alapján, követve a *Stocks on the Move* (Mozgásban lévő részvények) című könyvben leírt módszereket, amelyet **Andreas F. Clenow** írt. A szkript letölti és feldolgozza a történelmi részvényadatokat, exponenciális regressziót alkalmaz, majd egy Excel jelentést készít a kiválasztott részvényekről.

## Funkciók
- **S&P 500 komponensek lekérése** a SlickCharts weboldalról.
- **Történelmi árfolyamadatok lekérdezése** az Alpaca API és a Yahoo Finance segítségével.
- **Exponenciális regressziós trendek számítása** 90 napos időtávon.
- **Szűrési feltételek alkalmazása** az alábbiak szerint:
  - Momentum trend erőssége (**regressziós meredekség × R²**)
  - 100 napos mozgóátlag fölött van-e az árfolyam
  - Nagy árfolyamrések elkerülése (**>15% gap**)
  - Piaci kapitalizáció figyelembevétele
- **Excel riport generálása** színkódolt kiemelésekkel.

## Telepítés és követelmények
### Előfeltételek
Győződj meg róla, hogy a Python telepítve van, és az alábbi csomagok rendelkezésre állnak. Telepítéshez futtasd:

```sh
pip install yfinance pandas numpy scipy openpyxl alpaca-trade-api requests beautifulsoup4
```

### API Beállítások
A szkript futtatásához **Alpaca API kulcs** és a **Secret Key** szükséges a tőzsdei adatok eléréséhez. Frissítsd a következő sorokat a `main.py` fájlban a saját adataiddal:

```python
ALPACA_API_KEY = "YOUR_ALPACA_API_KEY"
ALPACA_SECRET_KEY = "YOUR_ALPACA_SECRET_KEY"
ALPACA_BASE_URL = "https://paper-api.alpaca.markets"
```

## Használat
A szkript futtatása az alábbi parancs kiadásával történik:

```sh
python main.py
```

### Kimenet
- A szkript egy **Excel fájlt (`momentum_strategy_output.xlsx`)** hoz létre, amely három munkalapot tartalmaz:
  1. **Filter** – A szűrési kritériumok alapján kiválasztott részvények listája.
  2. **Database** – Az összes elemzett részvény számítási eredményei.
  3. **Error** – Azok a részvények, amelyeknél az adatok hiányosak vagy nem voltak elérhetők.

## Szűrési Kritériumok (*Stocks on the Move* alapján)
A részvények kiválasztása az alábbi feltételeken alapul:
- **Momentum Erőssége**: Számítás az **exponenciális regresszió meredeksége × R²** képlettel.
- **100 napos mozgóátlag felett**: A részvénynek a 100 napos mozgóátlaga fölött kell zárnia.
- **Nagy árfolyamrések elkerülése**: Azok a részvények, amelyek **15%-nál nagyobb gap**-et mutattak, kiszűrésre kerülnek.
- **S&P 500 állapota**: Az indexnek önmagában a 200 napos mozgóátlaga felett kell lennie.

Ez a módszertan **Andreas F. Clenow** *Stocks on the Move* című könyvéből származik, amely a rendszeralapú momentum befektetési stratégiákra összpontosít.

