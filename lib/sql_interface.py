#!/usr/bin/env python
"""
SQLAlchemy wrapping of Instrument database

Main Class for full Database:  InstrumentDB

classes for Tables:
  Instrument
  Position
"""


import os
import json
import epics
import time
import socket
from threading import Thread
from datetime import datetime

from utils import backup_versions, save_backup, dumpsql

from sqlalchemy import MetaData, create_engine, and_, \
     Table, Column, Integer, Float, String, Text, DateTime, ForeignKey

from sqlalchemy.orm import sessionmaker,  mapper, clear_mappers, relationship, create_session
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import  NoResultFound


# needed for py2exe?
import sqlalchemy.dialects.sqlite

def isCollectorDB(dbname, server='sqlite'):
    """test if a file is a valid Instrument Library file:
       must be a sqlite db file, with tables named
          'info', 'instrument', 'position', 'pv',
       'info' table must have an entries named 'version' and 'create_date'
    """
    if not os.path.exists(dbname):
        return False
    try:
        engine = create_engine('%s:///%s' % (server, dbname))
        metadata =  MetaData(engine)
        metadata.reflect()
    except:
        return False

    if ('info' in metadata.tables and
        'instrument' in metadata.tables and
        'position' in metadata.tables and
        'pv' in metadata.tables):
        info = metadata.tables['info'].select().execute().fetchall()
        keys = [row.key for row in info]
        return ('version' in keys and 'create_date' in keys)
    return False

def json_encode(val):
    "simple wrapper around json.dumps"
    if val is None or isinstance(val, (str, unicode)):
        return val
    return  json.dumps(val)

def valid_score(score, smin=0, smax=5):
    """ensure that the input score is an integr
    in the range [smin, smax]  (inclusive)"""
    return max(smin, min(smax, int(score)))


def isotime2datetime(isotime):
    "convert isotime string to datetime object"
    sdate, stime = isotime.replace('T', ' ').split(' ')
    syear, smon, sday = [int(x) for x in sdate.split('-')]
    sfrac = '0'
    if '.' in stime:
        stime, sfrac = stime.split('.')
    shour, smin, ssec  = [int(x) for x in stime.split(':')]
    susec = int(1e6*float('.%s' % sfrac))

    return datetime(syear, smon, sday, shour, smin, ssec, susec)

def None_or_one(val, msg='Expected 1 or None result'):
    """expect result (as from query.all() to return
    either None or exactly one result
    """
    if len(val) == 1:
        return val[0]
    elif len(val) == 0:
        return None
    else:
        raise InstrumentDBException(msg)


class InstrumentDBException(Exception):
    """DB Access Exception: General Errors"""
    def __init__(self, msg):
        Exception.__init__(self)
        self.msg = msg
    def __str__(self):
        return self.msg


class _BaseTable(object):
    "generic class to encapsulate SQLAlchemy table"
    def __repr__(self):
        name = self.__class__.__name__
        fields = ['%s' % getattr(self, 'name', 'UNNAMED')]
        return "<%s(%s)>" % (name, ', '.join(fields))

class Info(_BaseTable):
    "general information table (versions, etc)"
    key, value = None, None
    def __repr__(self):
        name = self.__class__.__name__
        fields = ['%s=%s' % (getattr(self, 'key', '?'),
                             getattr(self, 'value', '?'))]
        return "<%s(%s)>" % (name, ', '.join(fields))

class Instrument(_BaseTable):
    "instrument table"
    name, notes = None, None

class Position(_BaseTable):
    "position table"
    pvs, instrument, instrument_id, date, name, notes = None, None, None, None, None, None

class Position_PV(_BaseTable):
    "position-pv join table"
    name, notes, pv, value = None, None, None, None
    def __repr__(self):
        name = self.__class__.__name__
        fields = ['%s=%s' % (getattr(self, 'pv', '?'),
                             getattr(self, 'value', '?'))]
        return "<%s(%s)>" % (name, ', '.join(fields))

class Command(_BaseTable):
    "command table"
    name, notes = None, None

class PVType(_BaseTable):
    "pvtype table"
    name, notes = None, None

class PV(_BaseTable):
    "pv table"
    name, notes = None, None

class Instrument_PV(_BaseTable):
    "intruemnt-pv join table"
    name, id, instrument, pv, display_order = None, None, None, None, None
    def __repr__(self):
        name = self.__class__.__name__
        fields = ['%s/%s' % (getattr(getattr(self, 'instrument', '?'),'name','?'),
                             getattr(getattr(self, 'pv', '?'), 'name', '?'))]
        return "<%s(%s)>" % (name, ', '.join(fields))

class Instrument_Precommand(_BaseTable):
    "instrument precommand table"
    name, notes = None, None

class Instrument_Postcommand(_BaseTable):
    "instrument postcommand table"
    name, notes = None, None

class InstrumentDB(object):
    "interface to Instrument Database"
    def __init__(self, dbname=None):
        self.dbname = dbname
        self.tables = None
        self.engine = None
        self.session = None
        self.conn    = None
        self.metadata = None
        self.update_mod_time = None
        if dbname is not None:
            self.connect(dbname)

    def create_newdb(self, dbname, connect=False):
        "create a new, empty database"
        backup_versions(dbname)
        make_newdb(dbname)
        if connect:
            time.sleep(0.5)
            self.connect(dbname, backup=False)

    def connect(self, dbname, server='sqlite', backup=True):
        "connect to an existing database"
        if not os.path.exists(dbname):
            raise IOError("Database '%s' not found!" % dbname)

        if not isInstrumentDB(dbname):
            raise ValueError("'%s' is not an Instrument file!" % dbname)

        if backup:
            save_backup(dbname)
        self.dbname = dbname
        self.engine = create_engine('%s:///%s' % (server, dbname))
        self.conn = self.engine.connect()
        self.session = sessionmaker(bind=self.engine)()

        self.metadata =  MetaData(self.engine)
        self.metadata.reflect()
        tables = self.tables = self.metadata.tables

        try:
            clear_mappers()
        except:
            pass

        mapper(Info,     tables['info'])
        mapper(Command,  tables['command'])
        mapper(PV,       tables['pv'])

        mapper(Instrument, tables['instrument'],
               properties={'pvs': relationship(PV,
                                               backref='instrument',
                                    secondary=tables['instrument_pv'])})

        mapper(PVType,   tables['pvtype'],
               properties={'pv':
                           relationship(PV, backref='pvtype')})

        mapper(Position, tables['position'],
               properties={'instrument': relationship(Instrument,
                                                      backref='positions'),
                           'pvs': relationship(Position_PV) })

        mapper(Instrument_PV, tables['instrument_pv'],
               properties={'pv':relationship(PV),
                           'instrument':relationship(Instrument)})

        mapper(Position_PV, tables['position_pv'],
               properties={'pv':relationship(PV)})

        mapper(Instrument_Precommand,  tables['instrument_precommand'],
               properties={'instrument': relationship(Instrument,
                                                      backref='precommands'),
                           'command':   relationship(Command,
                                                     backref='inst_precoms')})
        mapper(Instrument_Postcommand,   tables['instrument_postcommand'],
               properties={'instrument': relationship(Instrument,
                                                      backref='postcommands'),
                           'command':   relationship(Command,
                                                     backref='inst_postcoms')})

    def commit(self):
        "commit session state"
        self.set_mod_time()
        return self.session.commit()

    def close(self):
        "close session"
        self.clear_hostpid()
        self.session.commit()
        self.session.flush()
        self.session.close()

    def query(self, *args, **kws):
        "generic query"
        return self.session.query(*args, **kws)

    def get_info(self, key, default=None):
        """get a value from a key in the info table"""
        errmsg = "get_info expected 1 or None value for key='%s'"
        out = self.query(Info).filter(Info.key==key).all()
        thisrow = None_or_one(out, errmsg % key)
        if thisrow is None:
            return default
        return thisrow.value

    def set_info(self, key, value):
        """set key / value in the info table"""
        table = self.tables['info']
        vals  = self.query(table).filter(Info.key==key).all()
        if len(vals) < 1:
            # none found -- insert
            table.insert().execute(key=key, value=value)
        else:
            table.update(whereclause="key='%s'" % key).execute(value=value)

    def set_hostpid(self):
        """set hostname and process ID, as on intial set up"""
        self.set_info('host_name', socket.gethostname())
        self.set_info('process_id', str(os.getpid()))

    def clear_hostpid(self):
        """clear the hostname and process ID, as on shutdown"""
        self.set_info('host_name', '')
        self.set_info('process_id', 0)

    def check_hostpid(self):
        """check whether hostname and process ID match current config"""
        db_host_name = self.get_info('host_name', default='')
        db_process_id  = self.get_info('process_id', default='0')
        return ((db_host_name == '' and db_process_id == '0') or
                (db_host_name == socket.gethostname() and
                 db_process_id == str(os.getpid())))

    def set_mod_time(self):
        """set modify_date in info table"""
        if self.update_mod_time is None:
            self.update_mod_time = self.tables['info'].update(
                whereclause="key='modify_date'")
        self.update_mod_time.execute(value=datetime.isoformat(datetime.now()))

    def __addRow(self, table, argnames, argvals, **kws):
        """add generic row"""
        me = table() #
        for name, val in zip(argnames, argvals):
            setattr(me, name, val)
        for key, val in kws.items():
            if key == 'attributes':
                val = json_encode(val)
            setattr(me, key, val)
        try:
            self.session.add(me)
            # self.session.commit()
        except IntegrityError, msg:
            self.session.rollback()
            raise Warning('Could not add data to table %s\n%s' % (table, msg))

        return me



    def _get_foreign_keyid(self, table, value, name='name',
                           keyid='id', default=None):
        """generalized lookup for foreign key
arguments
    table: a valid table class, as mapped by mapper.
    value: can be one of the following
         table instance:  keyid is returned
         string:          'name' attribute (or set which attribute with 'name' arg)
            a valid id
            """
        if isinstance(value, table):
            return getattr(table, keyid)
        else:
            if isinstance(value, (str, unicode)):
                xfilter = getattr(table, name)
            elif isinstance(value, int):
                xfilter = getattr(table, keyid)
            else:
                return default
            try:
                query = self.query(table).filter(
                    xfilter==value)
                return getattr(query.one(), keyid)
            except (IntegrityError, NoResultFound):
                return default

        return default

    def get_all_instruments(self):
        """return instrument list
        """
        return [f for f in self.query(Instrument).order_by(Instrument.display_order)]

    def get_instrument(self, name):
        """return instrument by name
        """
        if isinstance(name, Instrument):
            return name
        out = self.query(Instrument).filter(Instrument.name==name).all()
        return None_or_one(out, 'get_instrument expected 1 or None Instrument')

    def get_ordered_instpvs(self, inst):
        """get ordered list of PVs for an instrument"""
        inst = self.get_instrument(inst)
        IPV = Instrument_PV
        return self.query(IPV).filter(IPV.instrument_id==inst.id
                                      ).order_by(IPV.display_order).all()


    def set_pvtype(self, name, pvtype):
        """ set a pv type"""
        pv = self.get_pv(name)
        out = self.query(PVType).all()
        _pvtypes = dict([(t.name, t.id) for t in out])
        if pvtype  in _pvtypes:
            pv.pvtype_id = _pvtypes[pvtype]
        else:
            self.__addRow(PVType, ('name',), (pvtype,))
            out = self.query(PVType).all()
            _pvtypes = dict([(t.name, t.id) for t in out])
            if pvtype  in _pvtypes:
                pv.pvtype_id = _pvtypes[pvtype]
        self.commit()

    def get_pv(self, name):
        """return pv by name
        """
        if isinstance(name, PV):
            return name
        out = self.query(PV).filter(PV.name==name).all()
        return None_or_one(out, 'get_pv expected 1 or None PV')

    def get_position(self, name, instrument=None):
        """return position from namea and instrument
        """
        inst = None
        if instrument is not None:
            inst = self.get_instrument(instrument)

        filter = (Position.name==name)
        if inst is not None:
            filter = and_(filter, Position.instrument_id==inst.id)

        out =  self.query(Position).filter(filter).all()
        return None_or_one(out, 'get_position expected 1 or None Position')

    def get_position_pv(self, name, instrument=None):
        """return position from namea and instrument
        """
        inst = None
        if instrument is not None:
            inst = self.get_instrument(instrument)

        filter = (Position.name==name)
        if inst is not None:
            filter = and_(filter, Position.instrument_id==inst.id)

        out =  self.query(Position).filter(filter).all()
        return None_or_one(out, 'get_position expected 1 or None Position')

    def add_instrument(self, name, pvs=None, notes=None,
                       attributes=None, **kws):
        """add instrument
        notes and attributes optional
        returns Instruments instance"""
        kws['notes'] = notes
        kws['attributes'] = attributes
        name = name.strip()
        inst = self.__addRow(Instrument, ('name',), (name,), **kws)
        if pvs is not None:
            pvlist = []
            for pvname in pvs:
                thispv = self.get_pv(pvname)
                if thispv is None:
                    thispv = self.add_pv(pvname)
                pvlist.append(thispv)
            inst.pvs = pvlist
        self.session.add(inst)
        self.commit()
        return inst

    def add_pv(self, name, notes=None, attributes=None, pvtype=None, **kws):
        """add pv
        notes and attributes optional
        returns PV instance"""
        out =  self.query(PV).filter(PV.name==name).all()
        if len(out) > 0:
            return

        kws['notes'] = notes
        kws['attributes'] = attributes
        row = self.__addRow(PV, ('name',), (name,), **kws)
        if pvtype is None:
            self.pvs[name] = epics.PV(name)
            self.pvs[name].get()
            pvtype = get_pvtypes(self.pvs[name])[0]
            self.set_pvtype(name, pvtype)

        self.session.add(row)
        self.commit()
        return row

    def add_info(self, key, value):
        """add Info key value pair -- returns Info instance"""
        row = self.__addRow(Info, ('key', 'value'), (key, value))
        self.commit()
        return row

    def remove_position(self, posname, inst):
        inst = self.get_instrument(inst)
        if inst is None:
            raise InstrumentDBException('Save Postion needs valid instrument')

        posname = posname.strip()
        pos  = self.get_position(posname, inst)
        if pos is None:
            raise InstrumentDBException("Postion '%s' not found for '%s'" %
                                        (posname, inst.name))

        tab = self.tables['position_pv']
        self.conn.execute(tab.delete().where(tab.c.position_id==pos.id))
        self.conn.execute(tab.delete().where(tab.c.position_id==None))

        tabl = self.tables['position']
        self.conn.execute(tabl.delete().where(tabl.c.id==pos.id))

        self.commit()

    def remove_instrument(self, inst):
        inst = self.get_instrument(inst)
        if inst is None:
            raise InstrumentDBException('Save Postion needs valid instrument')

        tab = self.tables['instrument']
        self.conn.execute(tab.delete().where(tab.c.id==inst.id))

        for tablename in ('position', 'instrument_pv', 'instrument_precommand',
                          'instrument_postcommand'):
            tab = self.tables[tablename]
            self.conn.execute(tab.delete().where(tab.c.instrument_id==inst.id))

    def save_position(self, posname, inst, values, **kw):
        """save position for instrument
        """
        inst = self.get_instrument(inst)
        if inst is None:
            raise InstrumentDBException('Save Postion needs valid instrument')

        posname = posname.strip()
        pos  = self.get_position(posname, inst)
        if pos is None:
            pos = Position()
            pos.name = posname
            pos.instrument = inst
            pos.date = datetime.now()

        pvnames = [pv.name for pv in inst.pvs]

        # check for missing pvs in values
        missing_pvs = []
        for pv in pvnames:
            if pv not in values:
                missing_pvs.append(pv)

        if len(missing_pvs) > 0:
            raise InstrumentDBException('Save Postion: missing pvs:\n %s' %
                                        missing_pvs)

        pos_pvs = []
        for name in pvnames:
            ppv = Position_PV()
            ppv.pv = self.get_pv(name)
            ppv.notes = "'%s' / '%s'" % (inst.name, posname)
            ppv.value = values[name]
            pos_pvs.append(ppv)
        pos.pvs = pos_pvs

        tab = self.tables['position_pv']
        self.conn.execute(tab.delete().where(tab.c.position_id == None))

        self.session.add(pos)
        self.commit()

    def restore_complete(self):
        "return whether last restore_position has completed"
        if len(self.restoring_pvs) > 0:
            return all([p.put_complete for p in self.restoring_pvs])
        return True

    def restore_position(self, posname, inst, wait=False, timeout=5.0,
                         exclude_pvs=None):
        """restore named position for instrument
        """

        inst = self.get_instrument(inst)
        if inst is None:
            raise InstrumentDBException(
                'restore_postion needs valid instrument')

        posname = posname.strip()
        pos  = self.get_position(posname, inst)
        if pos is None:
            raise InstrumentDBException(
                "restore_postion  position '%s' not found" % posname)

        # print 'Do Pre_Commands: ', inst.precommands
        pvvals = {}
        for pvpos in pos.pvs:
            pvvals[pvpos.pv.name] = str(pvpos.value)


        self.restoring_pvs = []
        if exclude_pvs is None:
            exclude_pvs = []
        epics_pvs = {}
        for pvname in pvvals:
            if pvname not in exclude_pvs:
                epics_pvs[pvname] =  epics.PV(pvname)

        for pvname, value in pvvals.items():
            if pvname not in exclude_pvs:
                thispv = epics_pvs[pvname]
                self.restoring_pvs.append(thispv)
                if not thispv.connected:
                    thispv.wait_for_connection()
                    thispv.get_ctrlvars()
                thispv.put(value, use_complete=True)


def PointerCol(name, other=None, keyid='id', **kws):
    if other is None:
        other = name
    return Column("%s_%s" % (name, keyid), None,
                  ForeignKey('%s.%s' % (other, keyid), **kws))

def StrCol(name, size=None, **kws):
    if size is None:
        return Column(name, Text, **kws)
    else:
        return Column(name, String(size), **kws)

def NamedTable(tablename, metadata, keyid='id', nameid='name',
               name=True, notes=True, attributes=True, cols=None):
    args  = [Column(keyid, Integer, primary_key=True)]
    if name:
        args.append(StrCol(nameid, nullable=False, unique=True))
    if notes:
        args.append(StrCol('notes'))
    if attributes:
        args.append(StrCol('attributes'))
    if cols is not None:
        args.extend(cols)
    return Table(tablename, metadata, *args)

class InitialData:
    info    = [["version", "0.1"],
               ["create_date", '<now>'],
               ["modify_date", '<now>']]

    commands = [['xas_scan',       'X-ray Absorption Spectra Scan, multi-segment'],
                ['line_scan',      'Simple Linear Stp Scan'],
                ['epics_scan',     'Load and Run a Scan with the Epics SScan Record'],
                ['fast_map',       'Continuous Map Scan'],
                ['webcam_collect', 'Save Image from URL'],
                ['xrf_collect',    'Expose and Save Integrated Spectra from XRF detector'],
                ['ad_collect',     'Expose and Save Image from Epics Area Detector'],
                ['moveto_pos',     'Move an Instrument to a named Position'],
                ['caput',          'Basic Epics caput'],
                ['caget',          'Basic Epics caget, storing value'],
                ['run_script',     'Run a Named Script of Commands'],
                ]
    status = ['requested', 'withdrawn', 'executing', 'completed', 'aborted', 'failed']

    pvtype = [['numeric',   'Numeric Value'],
              ['enum',      'Enumeration Value'],
              ['string',    'String Value'],
              ['motor',     'Motor Value']]


def  make_newdb(dbname, server='sqlite'):
    engine = create_engine('%s:///%s' % (server, dbname))
    metadata =  MetaData(engine)

    info = Table('info', metadata,
                 Column('key', Text, primary_key=True, unique=True),
                 StrCol('value'))


    instrument = NamedTable('instrument', metadata,
                            cols=[Column('show', Integer, default=1),
                                  Column('display_order', Integer, default=0)])

    position  = NamedTable('position', metadata,
                           cols=[Column('date', DateTime),
                                 PointerCol('instrument')])

    pvtype  = NamedTable('pvtype', metadata)
    pv      = NamedTable('pv', metadata, cols=[PointerCol('pvtype')])

    instrument_pv = Table('instrument_pv', metadata,
                          Column('id', Integer, primary_key=True),
                          PointerCol('instrument'),
                          PointerCol('pv'),
                          Column('display_order', Integer, default=0))

    position_pv = Table('position_pv', metadata,
                        Column('id', Integer, primary_key=True),
                        StrCol('notes'),
                        PointerCol('position'),
                        PointerCol('pv'),
                        StrCol('value'))

    command = NamedTable('command', metadata,
                         cols=[StrCol('parameters'),
                               PointerCol('cmd_type'),
                               StrCol('user_notes'),
                               StrCol('output_datafile'),
                               StrCol('result'),
                               Column('request_datetime', DateTime),
                               Column('finish_datetime', DateTime),
                               PointerCol('cmd_status')
                               ])

    cmd_types = NamedTable('cmd_type', metadata)
    cmd_status=  NamedTable('cmd_status', metadata)

    config = NamedTable('config', metadata,
                        cols=[Column('date', DateTime),
                                 PointerCol('instrument')])

    scan_info = NamedTable('scan_info', metadata,
                           cols=[Column('maxpts', Integer),
                                 PointerCol('command'),
                                 StrCol('ordinate_name'),
                                 StrCol('column_names') ])

    scan_data = NamedTable('scan_data', metadata,
                           cols=[Column('npt', Integer),
                                 PointerCol('scan_info'),
                                 StrCol('ordinate'),
                                 StrCol('columns') ])

    monitored_pvs = Table('monitored_pvs', metadata,
                          Column('id', Integer, primary_key=True),
                          Column('timestamp', DateTime),
                          StrCol('pvname'),
                          StrCol('value'))

    metadata.create_all()
    session = sessionmaker(bind=engine)()

    for name, notes in InitialData.commands:
        command.insert().execute(name=name, notes=notes)

    for name in InitialData.status:
        cmd_status.insert().execute(name=name)

    now = datetime.isoformat(datetime.now())

    for key, value in InitialData.info:
        if value == '<now>':
            value = now
        info.insert().execute(key=key, value=value)

    session.commit()


if __name__ == '__main__':
    dbname = 'Test.sdb'
    # backup_versions(dbname)
    make_newdb(dbname)
    print '''%s  created and initialized.''' % dbname
    dumpsql(dbname)
