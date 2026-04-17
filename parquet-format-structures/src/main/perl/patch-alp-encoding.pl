# Patch the generated Encoding.java to add ALP(10) support.
# ALP (Adaptive Lossless floating-Point) uses encoding value 10 (next after BYTE_STREAM_SPLIT=9).
# This patch should be removed once parquet-format includes ALP and parquet-java updates its dep.
use strict; use warnings;
my $file = $ARGV[0] or die "Usage: $0 <Encoding.java>\n";
open(my $fh, '<', $file) or die "Cannot read $file: $!";
my $content = do { local $/; <$fh> };
close($fh);
exit 0 if $content =~ /ALP/;  # already patched

# 1. Add ALP to the enum values list
$content =~ s/BYTE_STREAM_SPLIT\(9\);/BYTE_STREAM_SPLIT(9),\n\n  \/** ALP (Adaptive Lossless floating-Point) encoding for FLOAT and DOUBLE types.\n   * Encoding value 10. See https:\/\/github.com\/cwida\/ALP\n   *\/\n  ALP(10);/;

# 2. Add ALP to the findByValue switch
$content =~ s/(case 9:\s*\n\s*return BYTE_STREAM_SPLIT;)/$1\n      case 10:\n        return ALP;/;

open(my $out, '>', $file) or die "Cannot write $file: $!";
print $out $content;
close($out);
print "Patched $file with ALP(10)\n";
