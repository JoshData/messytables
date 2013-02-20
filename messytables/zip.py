import zipfile, StringIO

from messytables import TableSet

class ZIPTableSet(TableSet):
    """ Reads TableSets from inside a ZIP file """
    
    def __init__(self, tables):
        self._tables = tables

    @classmethod
    def from_fileobj(cls, fileobj):
        # zipfile requires a seekable stream, and actually one that can
        # handle .seek(offset, 2), i.e. seeking relative to the end of
        # the stream. messytables.seekable_stream only provides a stream
        # that supports seeking by absolute position and only near the
        # beginning. So we'll have to slurp the whole thing in.
        if not hasattr(fileobj, "seek"):
            fileobj = StringIO.StringIO(fileobj.read())
        
        from messytables.any import AnyTableSet # avoid circular dependency by not importing at the top
        tables = []
        found = []
        with zipfile.ZipFile(fileobj, 'r') as z:
            for f in z.infolist():
                ext = None
                if "." in f.filename: ext = f.filename[f.filename.rindex(".")+1:]
                
                try:
                    filetables = AnyTableSet.from_fileobj(z.open(f), extension=ext)
                except ValueError as e:
                    found.append(f.filename + ": " + e.message)
                    continue
                
                tables.extend(filetables.tables)
                
        if len(tables) == 0:
            raise ValueError("ZIP file has no recognized tables (%s)." % ", ".join(found))
                
        return ZIPTableSet(tables)
                
    @property
    def tables(self):
        """ Return the tables contained in any loadable files within the ZIP file. """
        return self._tables
        
