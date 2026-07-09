#!/usr/bin/env python
r"""merge_psd_xml.py -- combine per-IFO PSD XMLs into one multi-instrument PSD XML.

util_RIFT_pseudo_pipe.py's --use-online-psd-file takes a SINGLE file and assigns it to every
IFO (line 1136-1144), then ILE reads the per-instrument series from it
(lalsimutils.get_psd_series_from_xmldoc(file, inst) -> lal.series.read_psd_xmldoc(...)[inst]).
So a single XML containing BOTH H1 and L1 series lets a real multi-detector event use its true
per-IFO PSDs through the standard --use-online-psd-file plumbing -- no per-IFO PSD passthrough
needed. This merges the convert_psd_ascii2xml per-IFO outputs into that one file.

Usage:
  merge_psd_xml.py --out psd/combined-psd.xml.gz H1=psd/H1-psd.xml.gz L1=psd/L1-psd.xml.gz
"""
import argparse
import sys

import lal.series
from ligo.lw import utils as ligolw_utils


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True)
    ap.add_argument("assignments", nargs="+", help="IFO=path.xml.gz per detector")
    args = ap.parse_args()

    combined = {}
    for a in args.assignments:
        inst, path = a.split("=", 1)
        xmldoc = ligolw_utils.load_filename(path, contenthandler=lal.series.PSDContentHandler)
        d = lal.series.read_psd_xmldoc(xmldoc, root_name=None)
        # take the series for this instrument (single-IFO files key it by the IFO)
        if inst in d and d[inst] is not None:
            series = d[inst]
        else:
            # fall back to the sole non-None series in the file
            vals = [v for v in d.values() if v is not None]
            if len(vals) != 1:
                sys.exit("merge_psd_xml: %s has no series for %s and is not single-IFO" % (path, inst))
            series = vals[0]
        combined[inst] = series
        print("[merge] %s <- %s (deltaF=%.5f n=%d)" % (inst, path, series.deltaF, series.data.length))

    out_doc = lal.series.make_psd_xmldoc(combined)
    ligolw_utils.write_filename(out_doc, args.out)
    print("[merge] wrote %s with instruments %s" % (args.out, sorted(combined)))


if __name__ == "__main__":
    main()
