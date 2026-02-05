import argparse

def count_high(nmax=int(1e8)):
    """
    Function to count to a large number (example process for testing the execution of
    child processes).

    Parameters
    ----------
    nmax  int   Maximum number to count to
    """

    # Check for optional arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--nmax', default=1e8, help='Maximum number to count to')
    args = parser.parse_args()

    if args.nmax:
        nmax = int(float(args.nmax))

    i = 0
    for j in range(0, nmax, 1):
        i += 1

if __name__ == '__main__':
    count_high()
