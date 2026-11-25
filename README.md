# ctapipe_io_zfits

ctapipe io plugin for reading data in zfits files.

Requires Python 3.12 or newer and ctapipe 0.33.0 or newer.

Supports DL0v1 data as written by ACADA and R1v1 telescope data for commissioning.
`ProtozfitsEventSource` reads subarray DL0 data, while
`ProtozfitsTelescopeEventSource` reads individual R1 or DL0 telescope streams.
Both sources are available from `ctapipe_io_zfits.source` and the package root.

`MultiFiles` supports NectarCAM test data named
`NectarCAM.Run<obs_id:04d>.<chunk_id:04d>.fits.fz` via
`filename_convention="nectarcam"`. Even and odd chunks are parallel streams:
chunks 0/1, 2/3, etc. form pairs. With `all_chunks=True`, each stream advances by
two chunks and events are merged in event-id order. With `all_source_ids=False`,
only the input file's even or odd stream is read.

This `EventSource` implementation uses the `protozfits` python wrappers to the `adh-apis`
C++ project to read data written using ProtocolBuffers into zfits.
