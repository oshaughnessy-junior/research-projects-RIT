"""Exercise the real subdag writer with Condor-escaped waveform arguments."""
from pathlib import Path
import subprocess
import sys
import pytest

SCRIPT=Path(__file__).resolve().parents[2]/'bin/create_ile_sub_dag.py'

@pytest.mark.parametrize('cap,expected',[(101,2),(100,1)])
def test_quoted_kwargs_preserve_batch_size_and_point_cap(tmp_path,cap,expected):
    from igwn_ligolw import ligolw,lsctables,utils
    doc=ligolw.Document();root=doc.appendChild(ligolw.LIGO_LW())
    table=root.appendChild(lsctables.New(lsctables.SimInspiralTable,columns=['mass1','mass2']))
    for _ in range(101):
        row=table.RowType();row.mass1=30;row.mass2=20;table.append(row)
    grid=tmp_path/'grid.xml.gz';utils.write_filename(doc,str(grid),compress='gz')
    submit=tmp_path/'ILE.sub';submit.write_text('executable = /bin/true\narguments = "--internal-waveform-extra-lalsuite-args \'""{\'\'PhenomXPrecVersion\'\': 320}""\' --n-events-to-analyze 100"\ntransfer_executable = False\n')
    proc=subprocess.run([sys.executable,str(SCRIPT),'--sim-xml',str(grid),'--submit-script',str(submit),'--macroiteration','0','--target-dir',str(tmp_path),'--output-suffix','smoke','--cap-points',str(cap)],cwd=tmp_path,capture_output=True,text=True)
    assert proc.returncode==0,proc.stdout+proc.stderr
    assert "exe is /bin/true" in proc.stdout
    assert sum(line.startswith('JOB ') for line in (tmp_path/'iteration_0_smoke.dag').read_text().splitlines())==expected
