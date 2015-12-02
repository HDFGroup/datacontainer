from datetime import datetime
from datetime import timedelta
from collections import namedtuple


class DataPoint:
    """Base class describing a spatio-temporal properties of a data point.

    DataPoint objects can be created in two ways::

        >>> d = DataPoint(-77.45, 35.67, '2001-07-14T12:34:56')

    where all the arguments are positional in the order: latitude, longitude,
    time, or::

        >>> d = DataPoint(lat=-77.45, lon=35.67, time=datetime(2001, 7, 14, 12, 34, 56))

    with all named arguments. Mixing these two styles is not allowed.

    The time can be either an ISO 8601 string with no fractional seconds and
    time zone or a naive ``datetime()`` object.
    """

    def __init__(self, *args, **kwargs):
        if args and kwargs:
            raise RuntimeError(
                'Only all positional or all named arguments allowed')
        elif args:
            if len(args) != 3:
                raise RuntimeError('Three arguments expected: lat, lon, time')
            self._lat, self._lon, self._time = args
        elif kwargs:
            if any([k not in kwargs for k in ('lat', 'lon', 'time')]):
                raise RuntimeError(
                    'Three arguments "lat", "lon", and "time" expected')
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

    @property
    def filename(self):
        """Return the name of the file where the data point is stored."""
        raise NotImplementedError(
            'This method should be defined in a mixin data set class')

    def __repr__(self):
        return ('{:s}(lat={:f}, lon={:f}, time=\'{:s}\')'
                .format(type(self).__name__, self.lat, self.lon,
                        self.time.isoformat()))


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
        return timedelta(days=1)

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

    @property
    def grid_size_time(self):
        """Number of grid cells along the time axis."""
        return 7850

    @property
    def filename(self):
        """Return the name of the file where the data point is stored."""
        _fname_template = 'GSSTF_NCEP.3.{year:04d}.{month:02d}.{day:02d}.he5'
        dt = self.time
        return _fname_template.format(year=dt.year, month=dt.month, day=dt.day)


class GsstfDataPoint(GsstfNcep, DataPoint):
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

        # Compute geo bounding box...
        lons = [d.lon for d in dpoints]
        lats = [d.lat for d in dpoints]
        times = [d.time for d in dpoints]
        DataBBox = namedtuple(
            'DataBBox',
            ['min_lon', 'max_lon', 'max_lat', 'min_lat', 'begin', 'end'])
        self._databbox = DataBBox(min(lons), max(lons), max(lats), min(lats),
                                  min(times), max(times))

        # Compute dataspace bounding box...
        lons_idx = [d.lon_idx for d in dpoints]
        lats_idx = [d.lat_idx for d in dpoints]
        times_idx = [d.time_idx for d in dpoints]
        IndexBBox = namedtuple(
            'IndexBBox',
            ['min_lon', 'max_lon', 'max_lat', 'min_lat', 'begin', 'end'])
        self._idxbbox = IndexBBox(min(lons_idx), max(lons_idx),
                                  max(lats_idx), min(lats_idx),
                                  min(times_idx), max(times_idx))

        self._dpoints = dpoints

    @property
    def databbox(self):
        """Data bounding box as a namedtuple. The namedtuple's fields are:
        ``min_lon``, ``max_lon``, ``min_lat``, ``max_lat``, ``begin``, and
        ``end``.
        """
        return self._databbox

    @property
    def idxbbox(self):
        """Dataspace (index) bounding box as a namedtuple. The namedtuple's
        fields are: ``min_lon``, ``max_lon``, ``min_lat``, ``max_lat``,
        ``begin``, and ``end``.
        """
        return self._idxbbox

    def files(self):
        """A list of file names containing the data from the bounding box.

        :return: A generator that produces file names for the extent of the
            bounding box.

        Note the number of generated file names will equal the number of data
        points within the bounding box. There could be a lot of data points and
        many of their file names might be duplicates. Using Python's sets is
        recommended to only keep the unique file names, for example::

            >>> r = DataRegion([...])
            >>> unique_sorted_files = sorted(set(r.files()))
        """
        # The class of actual DataPoint objects...
        cls = type(self._dpoints[0])

        # Regenerate all the DataPoints within the data bounding box and return
        # their file names...
        idx = self.idxbbox
        time = self.databbox.begin
        for t in range(idx.begin, idx.end+1):
            lat = self._databbox.min_lat
            for lt in range(idx.min_lat, idx.max_lat+1):
                lon = self._databbox.min_lon
                for ln in range(idx.min_lon, idx.max_lon+1):
                    dp = cls(lat=lat, lon=lon, time=time)
                    yield dp.filename
                    lon += self.lon_res
                lat += self.lat_res
            time += self.time_res

    def bbox_dpoints(self):
        """Calculate the number of data points within the region's bounding
        box."""
        ix = self.idxbbox
        n = (ix.max_lon - ix.min_lon + 1) * (ix.max_lat - ix.min_lat + 1) * \
            (ix.end - ix.begin + 1)
        return n

    def __repr__(self):
        dbbox = self.databbox
        return ('<{:s}, {:d} data points, lon range={:f} degrees, '
                'lat range={:f} degrees, time range={}>'
                .format(type(self).__name__, self.bbox_dpoints(),
                        dbbox.max_lon - dbbox.min_lon,
                        dbbox.max_lat - dbbox.min_lat,
                        dbbox.end - dbbox.begin))


class GsstfDataRegion(GsstfNcep, DataRegion):
    """Define a data region of the GSSTF NCEP data set."""
    pass


class Cortad:
    """Describe the properties of the The Coral Reef Temperature Anomaly
    Database (CoRTAD) Version 5.

    http://data.nodc.noaa.gov/cgi-bin/iso?id=gov.noaa.nodc:0126774
    """
    # Data set's file name template (set to None because there are more than
    # one file)...
    _fname_template = None

    @property
    def lon_res(self):
        """Grid longitude resolution in degrees."""
        return 0.04165

    @property
    def lat_res(self):
        """Grid latitude resolution in degrees."""
        return 0.04165

    @property
    def time_res(self):
        """Grid temporal resolution."""
        return timedelta(weeks=1)

    @property
    def lat_max(self):
        """Grid maximum (northernmost) latitude in degrees."""
        return 89.9791717529297

    @property
    def lat_min(self):
        """Grid minimum (southernmost) latitude in degrees."""
        return -89.9791717529297

    @property
    def lon_max(self):
        """Grid maximum (easternmost) longitude in degrees."""
        return 179.979202270508

    @property
    def lon_min(self):
        """Grid minimum (westernmost) longitude in degrees."""
        return -179.979202270508

    @property
    def time_start(self):
        return datetime(1982, 1, 2)

    @property
    def time_end(self):
        return datetime(2012, 12, 29)

    @property
    def grid_size_lat(self):
        """Number of grid cells along the latitude axis."""
        return 4320

    @property
    def grid_size_lon(self):
        """Number of grid cells along the longitude axis."""
        return 8640

    @property
    def grid_size_time(self):
        """Number of grid cells along the time axis."""
        return 1617

    @property
    def filename(self):
        """Return the name of the file where the data point is stored."""
        raise NotImplementedError(
            'This method should be defined in a specific CoRTAD mixin class')


class CortadDataPoint(Cortad, DataPoint):
    """A data point of the CoRTAD data set."""
    pass


class CortadDataRegion(Cortad, DataRegion):
    """Describe CoRTAD data region."""
    pass
