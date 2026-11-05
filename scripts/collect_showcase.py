"""Single Ubuntu, four persistent Clients, bounded coordinated showcase collection."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import time
import uuid


def atomic(path, text):
    temp = path.with_suffix(path.suffix+'.tmp')
    temp.write_text(text)
    temp.replace(path)


def merge(results, t0, t1, group):
    if len(results) != 4 or any(r['t0_ns'] != t0 or r['t1_ns'] != t1 for r in results):
        raise RuntimeError('missing Client or differing windows')
    buckets = {}
    for r in results:
        if not (t0 <= r.get('actual_start_ns', t0) <= t0+100_000_000) or r.get('actual_end_ns',t1) < t1:
            raise RuntimeError('late start or early drain completion')
        if r['histogram_resolution_ms'] != .1:
            raise RuntimeError('histogram precision')
        accounted = sum(sum(r[k].values()) for k in ('window', 'drain')) + r.get('validation_error', 0) + r.get('pending_final', r.get('unfinished', 0)) + r.get('api_error', 0)
        if accounted != r['attempt']:
            raise RuntimeError('cohort ledger mismatch')
        if sum(count for _, count in r['histogram']) != r['window']['success']+r['drain']['success']:
            raise RuntimeError('histogram sample mismatch')
        for bucket, count in r['histogram']:
            buckets[bucket] = buckets.get(bucket, 0)+count
    total = sum(buckets.values())
    def quantile(q):
        seen = 0
        for bucket, count in sorted(buckets.items()):
            seen += count
            if seen >= math.ceil(total*q):
                return bucket*.1
        return None
    payload = {}
    for r in results:
        for key, count in r.get('payload', {}).items():
            payload[key] = payload.get(key, 0)+count
    both = group == 'rpc' or all(any(f'{ip}' in key and value > 0 for key, value in payload.items()) for ip in ('127.0.0.1:23000', '127.0.0.1:23001'))
    success = sum(r['window']['success'] for r in results)
    return {'valid': all(r['valid'] and r['normal_close'] for r in results) and both,
            'group': group, 't0_ns': t0, 't1_ns': t1, 'window_success': success,
            'success_qps': success/((t1-t0)/1e9),
            'success_MiB_s': sum(r.get('success_bytes', 0) for r in results)/1048576/((t1-t0)/1e9),
            'p50_ms': quantile(.5), 'p95_ms': quantile(.95), 'p99_ms': quantile(.99),
            'payload': payload, 'histogram': sorted(buckets.items()), 'clients': results}


def identity(pid):
    try:
        return Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()[19]
    except OSError:
        return None


def database_pids():
    found = {}
    for service in ('mysql', 'redis-server'):
        r = subprocess.run(['systemctl', 'show', service, '--property=MainPID', '--value'], capture_output=True, text=True)
        value = r.stdout.strip()
        found[service] = {'pid': int(value) if value.isdigit() and int(value) else None, 'diagnostic': r.stderr.strip()}
        if found[service]['pid']:
            found[service]['starttime'] = identity(found[service]['pid'])
    return found


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--build', required=True, type=Path)
    p.add_argument('--output', required=True, type=Path)
    p.add_argument('--config-dir', required=True, type=Path)
    p.add_argument('--run-dir', required=True, type=Path)
    p.add_argument('--groups', default='rpc,upload,download')
    p.add_argument('--smoke', action='store_true')
    p.add_argument('--tier', type=int, choices=(1, 2))
    p.add_argument('--repeat', type=int, choices=(1, 2, 3))
    a = p.parse_args()
    groups = a.groups.split(',')
    if not groups or any(g not in ('rpc', 'upload', 'download') for g in groups):
        p.error('groups must be rpc,upload,download')
    output = a.output.resolve()
    if str(output).startswith('/mnt/hgfs'):
        p.error('output must be Ubuntu local disk')
    output.mkdir(parents=True, exist_ok=False)
    if subprocess.run(['mountpoint', '-q', '/mnt/tnv/group001']).returncode:
        raise RuntimeError('business volume not mounted')
    def disk():
        spaces = {str(path): shutil.disk_usage(path).free for path in (output, Path('/mnt/tnv/group001'), a.build)}
        if min(spaces.values()) < 1024**3:
            raise RuntimeError('disk below 1GiB')
        return spaces
    disk()
    source = Path(__file__).resolve().parents[1]
    source_files = [f for base in (source/'src', source/'proto', source/'scripts', source.parent/'HighperRPC_01'/'src', source.parent/'HighperRPC_01'/'include') for f in base.rglob('*') if f.is_file() and f.suffix in ('.cpp','.cc','.h','.proto','.py','.sh')]
    source_files += [source/'CMakeLists.txt', source.parent/'HighperRPC_01'/'CMakeLists.txt']
    atomic(output/'source_sha256.json', json.dumps({str(f.relative_to(source.parent)): hashlib.sha256(f.read_bytes()).hexdigest() for f in source_files}, indent=2))
    atomic(output/'binary_sha256.json', json.dumps({f.name: hashlib.sha256(f.read_bytes()).hexdigest() for f in (a.build/'bin').iterdir() if f.is_file()}, indent=2))
    shutil.copyfile(a.build/'CMakeCache.txt', output/'CMakeCache.txt')
    env = {'clock': 'CLOCK_MONOTONIC', 'cpu_definition': 'one core=100%; RSS discrete samples', 'clk_tck': os.sysconf('SC_CLK_TCK'), 'page_size': os.sysconf('SC_PAGE_SIZE'), 'uname': list(os.uname()), 'dependencies': {}}
    for cmd in (['c++','--version'],['protoc','--version'],['mysql','--version'],['redis-server','--version']):
        r = subprocess.run(cmd,capture_output=True,text=True)
        env['dependencies'][cmd[0]] = r.stdout+r.stderr
    atomic(output/'environment.json', json.dumps(env, indent=2))
    # Save only numeric non-secret settings, never local config credentials.
    import re
    text = (a.config_dir/'tracker.cfg').read_text()
    atomic(output/'tracker_settings.json', json.dumps({key: re.findall(rf'(?m)^\s*{key}\s*=\s*(\d+)',text) for key in ('redis_key_timeout','tnv_rpc_workers','tnv_rpc_capacity','tnv_offline_seconds')}))
    services = json.loads((a.run_dir/'processes.json').read_text())
    databases = database_pids()
    atomic(output/'database_pids.json', json.dumps(databases,indent=2))
    service_ids = {r['pid']:r['identity'][1] for r in services}
    service_ids.update({r['pid']:r['starttime'] for r in databases.values() if r['pid']})
    suite = uuid.uuid4().hex
    seed_dir = output/'seed';seed_dir.mkdir()
    file_base = [str(a.build/'bin/tnv_file_bench'),'--dir',str(seed_dir),'--run-id',suite]
    seeded = False
    rounds = []
    try:
        if any(g != 'rpc' for g in groups):
            # Prepared once; bounded timeout. Manifest remains on failure.
            subprocess.run(file_base+['--mode','prepare'],check=True,timeout=300)
            seeded = True
            atomic(output/'seed_identity.json',json.dumps({'bytes_each':16777216,'count':8,'manifest_sha256':hashlib.sha256((seed_dir/'seeds.tsv').read_bytes()).hexdigest(),'source_sha256':hashlib.sha256((seed_dir/'source.bin').read_bytes()).hexdigest()}))
        for group in groups:
            for tier in ([a.tier] if a.tier else ([1] if a.smoke else [1,2])):
                for repeat in ([a.repeat] if a.repeat else ([1] if a.smoke else [1,2,3])):
                    folder = output/f'{group}-{tier}-{repeat}';folder.mkdir()
                    run = uuid.uuid4().hex
                    children = []
                    tokens = {}
                    handles = []
                    try:
                        for i in range(4):
                            result = folder/f'{i}.json'
                            common = ['--sync-dir',str(folder),'--run-id',run,'--client-id',str(i),'--output',str(result)]
                            if group == 'rpc':
                                args = [str(a.build/'bin/tnv_bench'),'--connections','4','--inflight',str(4 if tier==1 else 16),'--threads','1','--users','1000']+common
                            else:
                                args = [str(a.build/'bin/tnv_file_bench'),'--dir',str(seed_dir),'--mode',group,'--workers',str(tier),'--preferred','storage-a' if i<2 else 'storage-b']+common
                            log = (folder/f'{i}.log').open('w');handles.append(log)
                            child = subprocess.Popen(args,stdout=log,stderr=subprocess.STDOUT,stdin=subprocess.DEVNULL)
                            children.append(child);tokens[child.pid] = identity(child.pid)
                        ids = service_ids | tokens
                        deadline = time.monotonic()+300
                        phase = 'READY'
                        t0 = t1 = None
                        with (folder/'resources.jsonl').open('w') as resources:
                            while True:
                                sample = {'monotonic_ns':time.monotonic_ns(),'processes':{}}
                                try:
                                    sample['disk_free'] = disk()
                                except RuntimeError:
                                    atomic(folder/'STOP',run);raise
                                for pid, token in ids.items():
                                    try:
                                        if identity(pid) != token:
                                            sample['processes'][pid] = {'identity_changed': True};continue
                                        sample['processes'][pid] = {key: Path(f'/proc/{pid}/{key}').read_text() for key in ('stat','status')}
                                    except OSError as exc:
                                        sample['processes'][pid] = {'unavailable':str(exc)}
                                for key, path in [('cpu','/proc/stat'),('memory','/proc/meminfo'),('interfaces','/proc/net/dev')]:
                                    sample[key] = Path(path).read_text()
                                resources.write(json.dumps(sample)+'\n');resources.flush()
                                if all(c.poll() is not None for c in children):
                                    break
                                if any(c.poll() not in (None,0) for c in children):
                                    raise RuntimeError('Client failed; inspect logs/manifests')
                                if time.monotonic()>deadline:
                                    raise RuntimeError('coordination/drain deadline')
                                if phase in ('READY','WARMED') and all((folder/f'{i}.{phase}').exists() and (folder/f'{i}.{phase}').read_text()==run for i in range(4)):
                                    start = time.monotonic_ns()+2_000_000_000
                                    end = start+(10 if a.smoke or phase=='READY' else 60)*1_000_000_000
                                    atomic(folder/('warmup' if phase=='READY' else 'measure'), f'{run} {start} {end}')
                                    if phase=='WARMED':
                                        t0,t1 = start,end
                                    phase = 'WARMED' if phase=='READY' else 'DRAIN'
                                    deadline = end/1e9+150
                                time.sleep(1)
                        if any(c.returncode for c in children) or t0 is None:
                            raise RuntimeError('abnormal Client exit')
                        merged = merge([json.loads((folder/f'{i}.json').read_text()) for i in range(4)],t0,t1,group)
                        merged.update(tier=tier,repeat=repeat)
                        atomic(folder/'merged.json',json.dumps(merged,indent=2))
                        rounds.append(merged)
                        if not merged['valid']:
                            raise RuntimeError('invalid merged round')
                    except BaseException as exc:
                        atomic(folder/'STOP',run)
                        atomic(folder/'failure.json',json.dumps({'valid':False,'unfinished':True,'error':str(exc),'children':[c.pid for c in children]}))
                        raise
                    finally:
                        # Request stop first; only this parent's matching children can be terminated.
                        if any(c.poll() is None for c in children):
                            atomic(folder/'STOP',run)
                            end = time.monotonic()+10
                            while time.monotonic()<end and any(c.poll() is None for c in children):time.sleep(.1)
                        for c in children:
                            if c.poll() is None and identity(c.pid)==tokens[c.pid]:c.terminate()
                        for c in children:
                            try:c.wait(timeout=5)
                            except subprocess.TimeoutExpired:
                                if identity(c.pid)==tokens[c.pid]:c.kill()
                                c.wait(timeout=5)
                        for h in handles:h.close()
                    time.sleep(10)
        summary = []
        for group in groups:
            for tier in (1,2):
                subset = [r for r in rounds if r['group']==group and r['tier']==tier]
                if subset:
                    field = 'success_qps' if group=='rpc' else 'success_MiB_s'
                    values = [r[field] for r in subset]
                    summary.append({'group':group,'tier':tier,'metric':field,'median':statistics.median(values),'min':min(values),'max':max(values),'p99_median_ms':statistics.median(r['p99_ms'] for r in subset if r['p99_ms'] is not None) if any(r['p99_ms'] is not None for r in subset) else None})
        atomic(output/'summary.json',json.dumps(summary,indent=2))
        atomic(output/'summary.md','|group|tier|metric|median|min|max|\n|---|---:|---|---:|---:|---:|\n'+''.join(f"|{r['group']}|{r['tier']}|{r['metric']}|{r['median']}|{r['min']}|{r['max']}|\n" for r in summary))
    except BaseException as exc:
        atomic(output/'failure.json',json.dumps({'valid':False,'error':str(exc),'completed_rounds':len(rounds)}))
        raise
    finally:
        if seeded:
            r = subprocess.run(file_base+['--mode','cleanup'],timeout=180)
            atomic(output/'seed_cleanup.json',json.dumps({'success':r.returncode==0}))
            if r.returncode:raise RuntimeError('seed cleanup failed; manifest retained')

if __name__ == '__main__':
    main()
