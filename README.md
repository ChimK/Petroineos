# Petroineos
Petroineos Challenge

Issue I had:

I had an issue interfacing with the supplied C# Assemblies from Python with Pythonnet which I had used.  I would consistently get the error "No method matches given arguments" while passing a datetime value to the Methods when attempting to fetch data.

The error is described on this link (https://github.com/pythonnet/pythonnet/issues/1496) and appears to be a known issue.

In order to simply supply data to the pipeline (assuming that the pipeline is the key part of the challenge), I have put a small C# Console application together (please note it uses the external csvhelper libraries) which reads from the supplied assemblies and just writes a csv file to disk. I then feed that file to the Python application.

Executing the Python App means, however, it now needs two arguments:

main.py /location/of/csv/file/file.csv /folder/where/you/want/file/written/

I have also included a test load file generated from the C# assemblies which can be used to drive the pipeline.



