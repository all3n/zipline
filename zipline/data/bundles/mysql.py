import os
import pandas as pd
import numpy as np
import datetime
from logbook import Logger
from six import iteritems
from trading_calendars import register_calendar_alias
from zipline.utils.cli import maybe_show_progress
from . import core as bundles
from zipline.utils.yaml_utils import load_yaml
from zipline.utils.paths import zipline_path
from sqlalchemy import create_engine

log = Logger(__name__)
ZIPLINE_CONFIG_YAML = zipline_path(["config.yaml"])
zipline_cfg = load_yaml(ZIPLINE_CONFIG_YAML, False)
mysql_cfg = zipline_cfg['bundles']['mysql']


def mysql_engine(mysql_cfg: dict):
    mysql_port = mysql_cfg['port']
    mysql_name = mysql_cfg['name']
    mysql_password = mysql_cfg['password']
    mysql_host = mysql_cfg['host']
    mysql_database = mysql_cfg['database']
    engine = create_engine('mysql+mysqldb://{user}:{password}@{host}:{port}/{database}?charset=utf8'.format(
        user=mysql_name, password=mysql_password, host=mysql_host, port=mysql_port, database=mysql_database))
    return engine

engine = mysql_engine(mysql_cfg)



def load_data_mysql_jq(symbol, show_progress = False):
    data_table = pd.read_sql("SELECT * FROM daily_bars WHERE symbol = '%s'" % (symbol), con = engine)
    data_table.index = data_table.date
    return data_table




@bundles.register("mysql", calendar_name = 'XSHG')
def joinquant_bundle(
        environ,
        asset_db_writer,
        minute_bar_writer,
        daily_bar_writer,
        adjustment_writer,
        calendar,
        start_session,
        end_session,
        cache,
        show_progress,
        output_dir
        ):

    def parse_pricing_and_vol(
                          metadata,
                          symbols_map,
                          show_progress):

        with maybe_show_progress(symbols_map.keys(), show_progress, label = 'load data from db: ') as it:
            for sid, symbol in enumerate(it):
                dfr = load_data_mysql_jq(symbol, show_progress)
                start_date = dfr.index[0]
                end_date = dfr.index[-1]
                ac_date = end_date + pd.Timedelta(days=1)
                metadata.iloc[sid] = start_date, end_date, ac_date, symbol, symbols_map[symbol]
                new_index = ['open', 'high', 'low', 'close', 'volume']
                dfr = dfr.reindex(columns = new_index, copy=False) #fix bug
                sessions = calendar.sessions_in_range(start_date, end_date)
                dfr = dfr.reindex(
                    sessions.tz_localize(None),
                    copy = False
                ).fillna(0.0)
                yield sid, dfr
    sdf = pd.read_sql("select s.`symbol`,s.`display_name` from (SELECT DISTINCT(symbol) FROM `daily_bars`) t INNER JOIN `symbols` s WHERE s.symbol = t.symbol", con=engine)
    symbols_map = {}
    for index, row in sdf.iterrows():
        symbols_map[row.symbol] = row.display_name

    index_codes = ['000300.XSHG', "000001.XSHG", "399001.XSHE", "000905.XSHG"]
    idf = pd.read_sql(
        "SELECT * FROM `index` WHERE `symbol` IN (%s)" % (",".join(map(lambda a: '\'%s\'' % a, index_codes))),
        con=engine)

    for index, row in idf.iterrows():
        symbols_map[row.symbol] = row.display_name

    dtype = [('start_date', 'datetime64[ns]'),
                 ('end_date', 'datetime64[ns]'),
                 ('auto_close_date', 'datetime64[ns]'),
                 ('symbol', 'object'),
                 ('asset_name', 'object')
                 ]
    metadata = pd.DataFrame(np.empty(len(symbols_map), dtype=dtype))
    # http://docs.devhc.com/zipline/appendix/#zipline.data.bcolz_daily_bars.BcolzDailyBarWriter
    daily_bar_writer.write(
            parse_pricing_and_vol(
                metadata,
                symbols_map,
                show_progress
            ) ,
            show_progress=show_progress
        )

    metadata['exchange'] = 'joinquant'
    # http://docs.devhc.com/zipline/appendix/#zipline.assets.AssetDBWriter.write
    asset_db_writer.write(equities = metadata)

    adjustment_writer.write()


register_calendar_alias("mysql", "XSHG")

