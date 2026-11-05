"""Optional serial single-IO-thread collection, on Ubuntu only; never overwrites a run."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import time
p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--build', required=True, type=Path)
p.add_argument('--output', required=True, type=Path)
p.add_argument('--run-dir', required=True, type=Path)
p.add_argument('--config-dir', required=True, type=Path)
a = p.parse_args()
a.output.mkdir(parents=True, exist_ok=False)
source = Path(__file__).resolve().parents[1]
framework = source.parent/'HighperRPC_01'
files = [*source.rglob('*.cpp'), *source.rglob('*.h'), *source.rglob('*.proto'), *source.rglob('*.py'), *source.rglob('*.sh'), source/'CMakeLists.txt', *framework.rglob('*.cc'), *framework.rglob('*.h'), *framework.rglob('*.proto'), framework/'CMakeLists.txt']
(a.output/'source_sha256.json').write_text(json.dumps({str(f.relative_to(source.parent)): hashlib.sha256(f.read_bytes()).hexdigest() for f in files}, indent=2))
(a.output/'environment.txt').write_text('SC_CLK_TCK='+str(os.sysconf('SC_CLK_TCK'))+'\nSC_PAGE_SIZE='+str(os.sysconf('SC_PAGE_SIZE'))+'\n'+platform.platform()+'\n'+Path('/proc/cpuinfo').read_text()+'\n'+Path('/proc/meminfo').read_text())
for name, command in [('compiler',['c++','--version']),('protobuf',['protoc','--version'])]:
    r = subprocess.run(command,capture_output=True,text=True,check=True)
    (a.output/(name+'.txt')).write_text(r.stdout+r.stderr)
(a.output/'CMakeCache.txt').write_bytes((a.build/'CMakeCache.txt').read_bytes())
# Record only relevant non-secret timing settings; never copy local credentials.
import re
config = (a.config_dir/'tracker.cfg').read_text()
settings = {}
for key in ['redis_key_timeout', 'tnv_rpc_workers', 'tnv_rpc_capacity', 'tnv_offline_seconds']:
    m = re.search(rf'(?m)^\s*{key}\s*=\s*(\d+)', config)
    settings[key] = int(m[1]) if m else None
(a.output/'tracker_settings.json').write_text(json.dumps(settings, indent=2))
pids = [r['pid'] for r in json.loads((a.run_dir/'processes.json').read_text())]
summary = []
for connections, inflight in [(1,1),(4,4),(8,8),(16,16)]:
    qps = []
    for repeat in range(3):
        label = f'{connections}x{inflight}-{repeat+1}'
        output = a.output/(label+'.json')
        with (a.output/(label+'.log')).open('w') as log, (a.output/(label+'-resources.jsonl')).open('w') as resources:
            child = subprocess.Popen([str(a.build/'bin/tnv_bench'),'--connections',str(connections),'--inflight',str(inflight),'--threads','1','--warmup','5','--duration','30','--output',str(output)],stdout=log,stderr=subprocess.STDOUT)
            while child.poll() is None:
                sample = {'monotonic':time.monotonic(),'processes':{}}
                for pid in pids+[child.pid]:
                    try:
                        sample['processes'][str(pid)] = {'stat':Path(f'/proc/{pid}/stat').read_text(),'status':Path(f'/proc/{pid}/status').read_text()}
                    except OSError:
                        sample['processes'][str(pid)] = None
                resources.write(json.dumps(sample)+'\n');resources.flush();time.sleep(1)
            if child.returncode:
                raise RuntimeError(f'collection failed: {label}')
        result = json.loads(output.read_text())
        if not result['valid']:
            raise RuntimeError(f'invalid result: {label}')
        qps.append(result['success_qps'])
    summary.append({'connections':connections,'inflight':inflight,'median_qps':statistics.median(qps),'min_qps':min(qps),'max_qps':max(qps)})
(a.output/'summary.json').write_text(json.dumps(summary,indent=2))
