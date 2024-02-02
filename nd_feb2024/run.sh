#!/bin/bash

# install requirements
pip install -r requirements.txt

#find / -name "spark-submit" -type f 2>/dev/null
# use this command above to find the location for ./path/to/spark/bin/spark-submit

# Run spark-submit
#./path/to/spark/bin/spark-submit \
/Users/l.zhang/PycharmProjects/lz-pd-24/python10/lib/python3.10/site-packages/pyspark/bin/spark-submit \
  --master local[*] \
  pipeline.py
