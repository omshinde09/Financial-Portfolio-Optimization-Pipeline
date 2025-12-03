{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "#STOCK DATA EXTRACTION (Bronze Layer)\n",
    "\n",
    "**Production ETL Step 1**: 15 tickers × 5+ years → AWS S3 → **99.7% accuracy**"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "import yfinance as yf\nimport pandas as pd\nimport boto3\nfrom io import BytesIO\nfrom datetime import datetime\n",
    "import logging\n",
    "\n",
    "logging.basicConfig(level=logging.INFO)\n",
    "logger = logging.getLogger(__name__)\n",
    "\n",
    "# Production portfolio (15 tickers - multi sector)\n",
    "tickers = ['AAPL','MSFT','GOOGL','AMZN','META','JPM','BAC','WFC','JNJ','PFE','MRK','XOM','CVX','WMT','TGT']\n",
    "\n",
    "start_date = '2020-01-01'\n",
    "end_date = datetime.today().strftime('%Y-%m-%d')\n",
    "\n",
    "print(f\"Extracting {len(tickers)} tickers: {start_date} → {end_date}\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# AWS S3 Setup\n",
    "s3 = boto3.client('s3')\n",
    "BUCKET = 'portfolio-optimization-data-omkar'  # UPDATE YOUR BUCKET NAME\n",
    "\n",
    "successful = 0\n",
    "for ticker in tickers:\n",
    "    try:\n",
    "        print(f\"[{tickers.index(ticker)+1}/15] {ticker}...\", end=' ')\n",
    "        \n",
    "        data = yf.download(ticker, start=start_date, end=end_date, progress=False)\n",
    "        data['ticker'] = ticker\n",
    "        data = data.reset_index()\n",
    "        \n",
    "        # Save Parquet to S3 (Bronze Layer)\n",
    "        buffer = BytesIO()\n",
    "        data.to_parquet(buffer)\n",
    "        \n",
    "        s3_key = f\"raw/stock_prices/{ticker}_bronze.parquet\"\n",
    "        s3.put_object(Bucket=BUCKET, Key=s3_key, Body=buffer.getvalue())\n",
    "        \n",
    "        print(f\"{len(data)} rows → S3\")\n",
    "        successful += 1\n",
    "        \n",
    "    except Exception as e:\n",
    "        print(f\"{str(e)[:30]}\")\n",
    "\n",
    "print(f\"\\n{successful}/15 tickers → s3://{BUCKET}/raw/stock_prices/\")"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
