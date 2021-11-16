import os
import pandas as pd
import numpy as np
import datetime
from logbook import Logger
from six import iteritems
from trading_calendars import register_calendar_alias
from zipline.utils.cli import maybe_show_progress
from . import core as bundles

log = Logger(__name__)
if 'JQ_DATA_DIR' in os.environ:
    jq_data_dir = os.environ['JQ_DATA_DIR']
else:
    jq_data_dir = os.path.expanduser("~/jqdata")


def load_data_file_jq(f, show_progress = False):
    data_table = pd.read_csv(f, parse_dates = ['date'], index_col = 0)
    data_table.rename(columns = {
            "code": "symbol"
        }, inplace = True, copy = False)
    return data_table


def gen_asset_metadata(data, symbols_map, show_progress):
    if show_progress:
        log.info('Generating asset metadata.')


    data = data.groupby(
        by = 'symbol'
    ).agg(
        {'date': [np.min, np.max]}
    )
    data.reset_index(inplace = True)
    data['start_date'] = data.date.amin
    data['end_date'] = data.date.amax
    data['asset_name'] = data.symbol.map(lambda s: symbols_map[s])
    del data['date']
    data.columns = data.columns.get_level_values(0)

    data['exchange'] = 'joinquant'
    data['auto_close_date'] = data['end_date'].values + pd.Timedelta(days = 1)
    return data


@bundles.register("joinquant")
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

    def parse_pricing_and_vol(daily_dir,
                          daily_files,
                          metadata,
                          symbols_map,
                          show_progress):

        with maybe_show_progress(daily_files, show_progress, label = 'load joinquant files:') as it:
            for sid, f in enumerate(it):
                dfpath = os.path.join(daily_dir, f)
                symbol, name = f.replace(".csv", "").split("_")
                dfr = load_data_file_jq(dfpath, show_progress)


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


    # fetch data
    daily_dir = os.path.join(jq_data_dir, "daily")
    if not os.path.exists(daily_dir):
        daily_dir = jq_data_dir



    # read daily files
    daily_files = sorted(os.listdir(daily_dir))
    symbols_map = dict((f.replace(".csv", "").split("_")) for f in daily_files)


    dtype = [('start_date', 'datetime64[ns]'),
                 ('end_date', 'datetime64[ns]'),
                 ('auto_close_date', 'datetime64[ns]'),
                 ('symbol', 'object'),
                 ('asset_name', 'object')
                 ]
    metadata = pd.DataFrame(np.empty(len(daily_files), dtype=dtype))
    # http://docs.devhc.com/zipline/appendix/#zipline.data.bcolz_daily_bars.BcolzDailyBarWriter
    daily_bar_writer.write(
            parse_pricing_and_vol(
                daily_dir,
                daily_files,
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


register_calendar_alias("joinquant", "XSHG")

