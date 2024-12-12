import polars as pl
from ska_src_maltopuft_etl.meertrap.meertrap import extract, transform

from ska_src_maltopuft_etl.meertrap.candidate.transform import transform_candidate, transform_sp_candidate


def main() -> None:
    """ska-src-maltopuft-etl entrypoint."""
    raw_df = extract()
    obs_df, _ = transform(df=raw_df)

    #cand_df = transform_sp_candidate(
    #    candidate_df=transform_candidate(df=obs_df)
    #)
    subset = [
        "cand.dm",
        "cand.snr",
        "cand.ra",
        "cand.dec",
        "cand.width",
        "cand.mjd",
        "beam_id",
    ]
    obs_df = obs_df.sort(by="processed_at")

    print(obs_df[*subset, "host_id", "candidate"])
    sorted_df = (
        obs_df
        .unique(
            subset=subset,
            maintain_order=True,
            # Records are sorted by unix timestamp of candidate detection
            keep="first",
        )
    )
    print(sorted_df[*subset, "host_id", "candidate"])


if __name__ == "__main__":
    main()
