"""Extract ATNF pulsar catalogue data and load it into MALTOPUFT DB."""

from ska_src_maltopuft_etl.atnf.atnf import extract, load, transform


def main() -> None:
    """ETL routine for ATNF pulsar catalogue to MALTOPUFT DB."""
    atnf_df = extract()
    atnf_df = transform(df=atnf_df)
    load(df=atnf_df.to_pandas())


if __name__ == "__main__":
    main()
