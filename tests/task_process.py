import argparse

def count_high(nmax=int(1e8)):
    """
    Function to count to a large number (example process for testing the execution of
    child processes).

    Parameters
    ----------
    nmax  int   Maximum number to count to
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('file_path', help='Output file path')
    args = parser.parse_args()

    i = 0
    for j in range(0, nmax, 1):
        i += 1

if __name__ == '__main__':
    count_high()
