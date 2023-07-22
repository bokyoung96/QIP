from request_price import *

if __name__ == "__main__":
    start_time = time.time()

    start = '20230101'
    end = 'TODAY'

    request_split = RequestSplit(start=start, end=end)
    res = request_split.get_split_price()
    print(res)

    end_time = time.time()
    print('Time elapsed: {:.5f}sec'.format(end_time - start_time))
