#!/usr/bin/env python2

import argparse
import pandas as pd
import numpy as np
import random

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--min', type=int, help='Minimum service time (ns)', default=100)
    parser.add_argument('--max', type=int, help='Maximum service time (ns)', default=1000)
    parser.add_argument('--num', type=int, help='Number of samples to generate', default=1000)
    parser.add_argument('--filename', type=str, help='Output filename to use', default='uniform.csv')
    args = parser.parse_args()

    vals = [random.randint(args.min, args.max) for i in range(args.num)]
    df = pd.DataFrame({'values': vals})
    with open(args.filename, 'w') as f:
        f.write(df.to_csv(index=False))

if __name__ == '__main__':
    main()
