# ctapipe_io_zfits

ctapipe io plugin for reading data in zfits file (ACADA Rel 1 DL0, CTAO R1v1).

This `EventSource` implementation uses the `protozfits` python wrappers to the `adh-apis`
C++ project to read data written using ProtocolBuffers into zfits.

It is intendend to support reading
* LST / NectarCam R1 files using EVBv6 (using the CTAO R1v1 ProtocolBuffer definitions)
* ACADA Release 1 DL0 files (using the CTAO DL0v1 ProtocolBuffer definitions)
