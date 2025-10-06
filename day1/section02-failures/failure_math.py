#!/usr/bin/env python3
import math
def comb(n,k): return math.comb(n,k) if 0<=k<=n else 0
def avail(n,q,p): return sum(comb(n,i)*(p**i)*((1-p)**(n-i)) for i in range(q,n+1))
if __name__=='__main__':
  n=5;p=0.999;r=3;w=3
  print('N,R,W,p=',n,r,w,p)
  print('Read avail=',avail(n,r,p))
  print('Write avail=',avail(n,w,p))
