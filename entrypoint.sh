# TODO: Implement file based logging
python src/ska_src_maltopuft_etl/meertrap/main.py 2>&1 | tee logs/meertrap_$(date +%d.%m.%yT%H:%M:%S).log
python src/ska_src_maltopuft_etl/atnf/main.py 2>&1 | tee logs/atnf_$(date +%d.%m.%yT%H:%M:%S).log
