# ctapipe_io_zfits

ctapipe io plugin for reading data in zfits files.

At the moment, only DL0 data as written by ACADA is supported. If needed, it could be extended
to reading files containing R1 data for telescope commissioning, please open an issue to discuss.

This `EventSource` implementation uses the `protozfits` python wrappers to the `adh-apis`
C++ project to read data written using ProtocolBuffers into zfits.
