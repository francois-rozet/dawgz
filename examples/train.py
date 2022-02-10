#!usr/bin/env python

import time

from dawgz import job, after, waitfor, ensure, context, schedule

@job
def preprocessing():
    print('data preprocessing')
    
sequence = []
previous = preprocessing

for i in range(1, 4):
    @after(previous)
    @context(i=i)
    @job(name=f'train_{i}')
    def train():
        print(f'training step {i}')

    @after(train)
    @context(i=i)
    @job(name=f'eval_{i}')
    def evaluate():
        print(f'evaluation step {i}')

    sequence.append((train, evaluate))
    previous = train

evals = [e for t, e in sequence]

if __name__ == '__main__':
    schedule(*evals, backend='async')
