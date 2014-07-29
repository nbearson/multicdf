#!/usr/bin/env python
"""
A basic proof-of-concept to discover the feasability
of taking snapshots from a NetCDF file while it's
being written to.

Usage: multicdf.py input.jpg output.nc 
"""

from PIL import Image
import numpy as np
from netCDF4 import Dataset
from multiprocessing import Process
from progress.bar import Bar

SDSNAME="image"
VERIFYFREQ=10 # we spawn a process to read & check every X lines
DEBUG=False  # set to true for single process testing

def img2arr(img):
  im = Image.open(img)
  im = im.convert('1') # converts to black & white
  return np.asarray(im.getdata()).reshape(im.size[::-1]) # pillow .size is reversed

def initializeDataset(cdf, shape):
  r = Dataset(cdf, 'w', format='NETCDF4')
  x = r.createDimension('x', shape[0])
  y = r.createDimension('y', shape[1])
  sds = r.createVariable(SDSNAME, 'i4', ('x', 'y')) 
  r.close()

def writeLine(cdf, image, i):
  r = Dataset(cdf, 'a', format='NETCDF4')
  var = r.variables[SDSNAME]
  var[i] = image[i]
  r.close()

def readToLine(cdf, i):
  r = Dataset(cdf, 'r', format='NETCDF4')
  var = r.variables[SDSNAME]
  lines = var[:i]
  r.close()
  return lines 

def verify(cdf, image, i):
  d = readToLine(cdf, i)
  if not (d == image[:i]).all():
    print "*** no match", i, "***"

if __name__ == "__main__":
  import sys
  _, img, cdf = sys.argv
  grey = img2arr(img)
  print "Image size:", grey.shape
  initializeDataset(cdf, grey.shape)
  for i in Bar('Processing line').iter(range(len(grey))):
    writeLine(cdf, grey, i)
    if i % VERIFYFREQ == 0:
      if DEBUG:
        verify(cdf, grey, i)
      else:
        p = Process(target=verify, args=(cdf, grey, i))
        p.start()
