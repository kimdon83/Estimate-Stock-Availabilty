# import time


# def heavy_work(name):
#     result = 0
#     for i in range(4000000):
#         result += i
#     print('%s done' % name)


# if __name__ == '__main__':
#     import multiprocessing

#     start = time.time()
#     procs = []
#     for i in range(4):
#         p = multiprocessing.Process(target=heavy_work, args=(i, ))
#         p.start()
#         procs.append(p)

#     for p in procs:
#         p.join()  # 프로세스가 모두 종료될 때까지 대기

#     end = time.time()

#     print("수행시간: %f 초" % (end - start))


import time

def heavy_work(name):
    result = 0
    for i in range(4000000):
        result += i
    print('%s done' % name)

if __name__ == '__main__':
    import multiprocessing

    start = time.time()
    pool = multiprocessing.Pool(processes=4)
    pool.map(heavy_work, range(4))
    pool.close()
    pool.join()

    end = time.time()

    print("수행시간: %f 초" % (end - start))    