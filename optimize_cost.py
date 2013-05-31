from math import ceil

PACKED_VALUES = 504
def find_inflection(bitwidth):
  rle_cost = float(16 + (ceil(bitwidth/8.0)*8) + (PACKED_VALUES * bitwidth))
  bp_cost = float(8 + (bitwidth * PACKED_VALUES))
  cpb_bp = bp_cost / PACKED_VALUES

  repeats = 2
  while(True):
    cpb_rle = (rle_cost / (PACKED_VALUES + repeats))
    if cpb_rle < cpb_bp: break
    repeats += 1
  print "Bitwidth: %s, repeats %s" % (bitwidth, repeats)

for bitwidth in xrange(1, 33):
  find_inflection(bitwidth)

