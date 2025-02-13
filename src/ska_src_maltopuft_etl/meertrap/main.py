"""ska-src-maltopuft-etl entrypoint."""

from ska_src_maltopuft_etl.meertrap.meertrap import load, parse, transform


def main() -> None:
    """ska-src-maltopuft-etl entrypoint."""
    obs_df, cand_df = parse()
    obs_df, cand_df = transform(obs_df=obs_df, cand_df=cand_df)
    obs_df, cand_df = load(obs_df=obs_df, cand_df=cand_df)


if __name__ == "__main__":
    main()
