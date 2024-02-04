#!/bin/bash

# install requirements
pip install -r requirements.txt

# use this command to find the location for ./path/to/spark/bin/spark-submit
#find / -name "spark-submit" -type f 2>/dev/null

# Run spark-submit
# Terminal be in directory nd_feb2024, bash run.sh to execute
./path/to/spark/bin/spark-submit \
  --master local[*] \
  pipeline.py
