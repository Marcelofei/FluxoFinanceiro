"""Consistent, checksummed snapshots; imports validated before any destructive SQL."""
import datetime
import hashlib
import io
import json
import zipfile
import pandas as pd

TABLES = ('categorias_personalizadas','cartoes','faturas','lancamentos','info_dividas',
          'reserva_emergencia','pagamentos','recorrencias_geradas','orcamentos_categorias',
          'preferencias_app','auditoria')


def export_snapshot(conn):
    files = {}
    # One connection and one repeatable-read transaction for the whole snapshot.
    with conn.cursor() as cur:
        cur.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
        for table in TABLES:
            cur.execute('SELECT * FROM '+table)
            columns = [c[0] for c in cur.description]
            frame = pd.DataFrame(cur.fetchall(),columns=columns)
            for col in ('ajuste_pagamento_ids','anterior','posterior'):
                if col in frame:
                    frame[col] = frame[col].map(lambda v: json.dumps(v,ensure_ascii=False,default=str) if isinstance(v,(dict,list)) else v)
            files[table+'.csv'] = frame.to_csv(index=False).encode('utf-8')
    metadata={'schema_version':4,'format':'gestao_financeira_full_backup','created_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'tables':list(files),'sha256':{name:hashlib.sha256(data).hexdigest() for name,data in files.items()}}
    buffer=io.BytesIO()
    with zipfile.ZipFile(buffer,'w',compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('metadata.json',json.dumps(metadata))
        for name,data in files.items(): zf.writestr(name,data)
    return buffer.getvalue()


def read_snapshot(arquivo):
    arquivo.seek(0)
    with zipfile.ZipFile(arquivo) as zf:
        if sum(i.file_size for i in zf.infolist()) > 100*1024*1024:
            raise ValueError('Backup excede o limite de 100 MB descompactados')
        if len(zf.namelist()) != len(set(zf.namelist())):
            raise ValueError('Backup contém arquivos duplicados')
        metadata=json.loads(zf.read('metadata.json')) if 'metadata.json' in zf.namelist() else {}
        version=metadata.get('schema_version',1)
        if version not in (1,2,3,4): raise ValueError('Versão de backup não suportada')
        required = set(TABLES) if version==4 else {'lancamentos','categorias_personalizadas','info_dividas','reserva_emergencia'}
        if version==3: required |= {'pagamentos','recorrencias_geradas','orcamentos_categorias','preferencias_app'}
        if any(t+'.csv' not in zf.namelist() for t in required):
            raise ValueError('Backup incompleto; nenhum dado foi alterado')
        frames={}
        for table in TABLES:
            name=table+'.csv'
            if name not in zf.namelist(): frames[name]=pd.DataFrame(); continue
            data=zf.read(name)
            if version==4 and hashlib.sha256(data).hexdigest()!=metadata.get('sha256',{}).get(name):
                raise ValueError('Checksum inválido: '+name)
            frames[name]=pd.read_csv(io.BytesIO(data))
            for col in ('ajuste_pagamento_ids','anterior','posterior'):
                if col in frames[name]:
                    frames[name][col]=frames[name][col].map(lambda v: json.loads(v) if isinstance(v,str) and v.strip() else None)
        return frames
