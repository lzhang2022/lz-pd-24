#!/bin/bash

#find / -name "spark-submit" -type f 2>/dev/null


# Run spark-submit
#./path/to/spark/bin/spark-submit \
/Users/l.zhang/PycharmProjects/lz-pd-24/python10/lib/python3.10/site-packages/pyspark/bin/spark-submit \
  --master local[*] \
  pipeline.py
