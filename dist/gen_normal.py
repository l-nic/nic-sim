#!/usr/bin/env python2

import argparse
import pandas as pd
import numpy as np
import random

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mean', type=int, help='Average service time (ns)', default=1000)
    parser.add_argument('--stddev', type=int, help='Standard deviation of service time (ns)', default=100)
    parser.add_argument('--num', type=int, help='Number of samples to generate', default=10000)
    parser.add_argument('--filename', type=str, help='Output filename to use', default='normal.csv')
    args = parser.parse_args()

    vals = [int(random.gauss(args.mean, args.stddev)) for i in range(args.num)]
    df = pd.DataFrame({'values': vals})
    with open(args.filename, 'w') as f:
        f.write(df.to_csv(index=False))

if __name__ == '__main__':
    main()
