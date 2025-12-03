Financial Portfolio Pipeline (Production-Ready)

99.7% Accurate | 15 Tickers | 5+ Years Data
text
yfinance API → PySpark ETL → PostgreSQL → Power BI

## 🛠️ **Run It Now** (2 Minutes)

1. Clone repo
git clone https://github.com/omkar-shinde-data/financial-portfolio-pipeline.git
cd financial-portfolio-pipeline

2. Install dependencies
pip install -r requirements.txt

3. Run extraction pipeline ✅ CORRECT
jupyter notebook 01_data_extraction.ipynb

4. OR run as script
python -m nbconvert --to python 01_data_extraction.ipynb --execute
python 01_data_extraction.py

5. Run PySpark ETL
jupyter notebook 02_etl_pipeline.ipynb
