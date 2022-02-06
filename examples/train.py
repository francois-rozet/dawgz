#!usr/bin/env python

import time

from dawgz import job, after, waitfor, ensure, schedule

@job
def preprocessing():
    print('data preprocessing')
    
seq = []

for i in range(1, 4):
    if seq:
        dep = seq[-1][0]
    else:
        dep = preprocessing

    @after(dep)
    @job(name=f'train_{i}')
    def train(i: int = i):  # fixes the value of i locally
        print(f'training step {i}')

    @after(train)
    @job(name=f'eval_{i}')
    def evaluate(i: int = i):
        print(f'evaluation step {i}')

    seq.append((train, evaluate))

evals = [e for t, e in seq]

if __name__ == '__main__':
    schedule(*evals, backend='async')
