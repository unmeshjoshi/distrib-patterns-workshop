#!/usr/bin/env python3
import random
def sim(n,q,p,trials=100000):
  ok=0
  for _ in range(trials):
    up=sum(1 for _ in range(n) if random.random()<p)
    if up>=q: ok+=1
  return ok/trials
if __name__=='__main__':
  n=5;p=0.999;r=3;w=3
  print('Monte Carlo R=',sim(n,r,p),' W=',sim(n,w,p))
