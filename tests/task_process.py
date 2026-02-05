
def count_high(nmax=int(2e8)):
    """
    Function to count to a large number (example process for testing the execution of
    child processes).

    Parameters
    ----------
    nmax  int   Maximum number to count to
    """

    i = 0
    for j in range(0, nmax, 1):
        i += 1

if __name__ == '__main__':
    count_high()
