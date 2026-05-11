
def output_ds9_overlay(catalog, file_path, format='table', radius=None, colour='green', xcol='x', ycol='y'):
    """Function to output from an astropy Table that includes x,y
    image coordinates as Columns named 'x' and 'y' as an region overlay
    file for use in DS9

    :param catalog:  Table or array
    :param file_path: Path to the output file
    :param format:  [optional, default=table] Indicates either table or array
    :param radius: [optional, default=None ie point] Radius of circle used in output
    :param colour: [optional, default=green] Colour of overlay
    :param xcol: [optional, default='x'] Name or index of x-position column in table or array
    :param ycol: [optional, default='y'] Name or index of x-position column in table or array
    """

    with open(file_path,'w') as f:
        for j in range(0,len(catalog),1):

            if format == 'table':
                if radius != None:
                    f.write('circle '+str(catalog[xcol][j])+' '+str(catalog[ycol][j])+
                        ' '+str(radius)+' # color='+colour+'\n')

                else:
                    f.write('point '+str(catalog[xcol][j])+' '+str(catalog[ycol][j])+
                        ' # color='+colour+'\n')
            else:
                if radius != None:
                    f.write('circle ' + str(catalog[:,xcol][j]) + ' ' + str(catalog[:,ycol][j]) +
                            ' ' + str(radius) + ' # color=' + colour + '\n')

                else:
                    f.write('point ' + str(catalog[:,xcol][j]) + ' ' + str(catalog[:,ycol][j]) +
                            ' # color=' + colour + '\n')
    f.close()
