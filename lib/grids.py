from datetime import datetime
from datetime import timedelta
from collections import namedtuple


class DataPoint:
    """Base class describing a spatio-temporal properties of a data point."""

    def __init__(self, *args, **kwargs):
        if args and kwargs:
            raise RuntimeError(
                'Only all positional or all named arguments allowed')
        elif args:
            if len(args) != 3:
                raise RuntimeError('Two arguments expected')
            self._lat, self._lon, self.time = args
        elif kwargs:
            if any([k not in kwargs for k in ('lat', 'lon', 'time')]):
                raise RuntimeError('Two arguments "lat" and "lon" expected')
            self._lat = kwargs['lat']
            self._lon = kwargs['lon']
            self._time = kwargs['time']

        # Check input lat/lon...
        if not (self.lat_max >= self._lat >= self.lat_min):
            raise ValueError('Latitude out of range: %s' % self._lat)
        if not (self.lon_max > self._lon >= self.lon_min):
            raise ValueError('Longitude out of range: %s' % self._lon)

        # Check input time...
        if not isinstance(self._time, (str, datetime)):
            raise TypeError('Time can be given as string or datetime object')
        if isinstance(self._time, str):
            self._time = datetime.strptime(self._time, '%Y-%m-%dT%H:%M:%S')
        if not (self.time_end > self._time >= self.time_start):
            raise ValueError('Time out of range: %s' % self._time.isoformat())

    @property
    def lat(self):
        return self._lat

    @property
    def lon(self):
        return self._lon

    @property
    def time(self):
        return self._time

    @property
    def lat_idx(self):
        return int((self._lat - self.lat_min) / self.lat_res)

    @property
    def lon_idx(self):
        return int((self._lon - self.lon_min) / self.lon_res)

    @property
    def time_idx(self):
        return int((self._time - self.time_start) / self.time_res)


class GsstfNcep:
    """Describe the properties of the NCEP/DOE Reanalysis II,
    for GSSTF, 0.25x0.25 deg, Daily Grid, v3 data set.

    https://catalog.data.gov/dataset/ncep-doe-reanalysis-ii-for-gsstf-0-25x0-25-deg-daily-grid-v3-gsstf-ncep-at-ges-disc-v3
    """

    @property
    def lon_res(self):
        """Grid longitude resolution in degrees."""
        return 0.25

    @property
    def lat_res(self):
        """Grid latitude resolution in degrees."""
        return 0.25

    @property
    def time_res(self):
        """Grid temporal resolution."""
        return timedelta(1)

    @property
    def lat_max(self):
        """Grid maximum (northernmost) latitude in degrees."""
        return 90.

    @property
    def lat_min(self):
        """Grid minimum (southernmost) latitude in degrees."""
        return -90.

    @property
    def lon_max(self):
        """Grid maximum (easternmost) longitude in degrees."""
        return 180.

    @property
    def lon_min(self):
        """Grid minimum (westernmost) longitude in degrees."""
        return -180.

    @property
    def time_start(self):
        return datetime(1987, 7, 1)

    @property
    def time_end(self):
        return datetime(2009, 1, 1)

    @property
    def grid_size_lat(self):
        """Number of grid cells along the latitude axis."""
        return 720

    @property
    def grid_size_lon(self):
        """Number of grid cells along the longitude axis."""
        return 1440


class NcepDataPoint(GsstfNcep, DataPoint):
    """Define a data point of the GSSTF NCEP data set."""
    pass


class DataRegion:
    """Base class for describing a region in the data space defined by a set of
    DataPoint objects.
    """

    def __init__(self, dpoints):
        """Define a data region with a set of DataPoint objects.

        :arg list dpoints: A list with DataPoint objects.
        """
        if len(dpoints) < 2:
            raise RuntimeError(
                'Data region not defined (min 2 DataPoint objects required)')
        if not all([isinstance(d, DataPoint) for d in dpoints]):
            raise TypeError('All region points must be DataPoint objects')

        # Compute bounding box (geo and dataspace)...
        lons = [d.lon for d in dpoints]
        lats = [d.lat for d in dpoints]
        times = [d.time for d in dpoints]
        GeoBBox = namedtuple(
            'GeoBBox',
            ['west', 'east', 'north, ''south', 'begin', 'end'])
        self._geobbox = GeoBBox(min(lons), max(lons), max(lats), min(lats),
                                min(times), max(times))
        lons_idx = [d.lon_idx for d in dpoints]
        lats_idx = [d.lat_idx for d in dpoints]
        times_idx = [d.time_idx for d in dpoints]
        IndexBBox = namedtuple(
            'IndexBBox',
            ['west', 'east', 'north, ''south', 'begin', 'end'])
        self._idxbbox = IndexBBox(min(lons_idx), max(lons_idx),
                                  max(lats_idx), min(lats_idx),
                                  min(times_idx), max(times_idx))

    @property
    def geobbox(self):
        """Geo bounding box as a namedtuple."""
        return self._geobbox

    @property
    def idxbbox(self):
        """Dataspace (index) bounding box as a namedtuple."""
        return self._idxbbox
