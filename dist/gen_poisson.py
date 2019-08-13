#!/usr/bin/env python2

import argparse
import pandas as pd
import numpy as np
import random

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--lam', type=int, help='Expectation of interval (ns), should be >= 0', default=400)
    parser.add_argument('--num', type=int, help='Number of samples to generate', default=10000)
    parser.add_argument('--filename', type=str, help='Output filename to use', default='poisson.csv')
    args = parser.parse_args()

    vals = np.random.poisson(args.lam, args.num) 
    df = pd.DataFrame({'values': vals})
    with open(args.filename, 'w') as f:
        f.write(df.to_csv(index=False))

if __name__ == '__main__':
    main()
