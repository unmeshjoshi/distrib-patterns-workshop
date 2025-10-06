#!/usr/bin/env python3
import argparse, math
def comb(n,k): return math.comb(n,k) if 0<=k<=n else 0
def avail(n,q,p): return sum(comb(n,i)*(p**i)*((1-p)**(n-i)) for i in range(q,n+1))
ap=argparse.ArgumentParser(); ap.add_argument('--n',type=int,required=True); ap.add_argument('--r',type=int,required=True); ap.add_argument('--w',type=int,required=True); ap.add_argument('--p',type=float,default=0.999)
a=ap.parse_args()
print('Read avail=',avail(a.n,a.r,a.p)); print('Write avail=',avail(a.n,a.w,a.p))
print('Rules: need R> N/2, W> N/2 and R+W> N for intersecting quorums. For f failures: N=2f+1, R=W=f+1.')
