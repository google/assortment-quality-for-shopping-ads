# 📦 Assortment Quality - Product and Brand coverage monitoring

**This is not an officially supported Google product.**

Assortment Quality is an open-source solution that gives you an overview of the product and brand coverage of your
Google Merchant center account.

## Intended audience

Google Cloud Platform with BigQuery and BigQuery Data Transfer access

> It is assumed that the user is familiar with Google Cloud
> Platform

## Pre-requisites

- BQ Command line OR activate the API from the Console UI
- Enable Drive / BQ / BQDT Api

## Installation

- Clone this repo locally or use our
  [Colab notebook](https://colab.research.google.com/github/google/assortment-quality-for-shopping-ads/blob/main/Colab%20-%20Setup%20Guide.ipynb)
- Run the following command  (all parameters are mandatory):
```bash
python main.py
  -p project_id] -m [merchant_id] -r [region_name]
  -d [dataset_name] -l [language] -c [country] -e [expiration_time]
```



## Authors

 - [François Pérez](mailto:fraperez@google.com)

## Licensing

Terms of the release - Copyright 2021 Google LLC. Licensed under the Apache
License, Version 2.0.