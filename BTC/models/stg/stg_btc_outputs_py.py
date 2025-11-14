import pandas


def model(dbt, session):

	dbt.config(materialized="table", packages=["pandas"])
	
	df = dbt.ref("stg_btc").to_pandas()
	
	return df