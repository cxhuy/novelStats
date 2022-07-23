import munpia, navernovel, novelpia, kakaostage, kakaopage, multiprocessing

p1 = multiprocessing.Process(target=munpia.startMunpiaCrawling)
p2 = multiprocessing.Process(target=navernovel.startNavernovelCrawling)
p3 = multiprocessing.Process(target=novelpia.startNovelpiaCrawling)
p4 = multiprocessing.Process(target=kakaostage.startKakaostageCrawling)
p5 = multiprocessing.Process(target=kakaopage.startKakaopageCrawling)

if __name__ == "__main__":
    multiprocessing.set_start_method('spawn')
    p1.start()
    p2.start()
    p3.start()
    p4.start()
    p5.start()
    p1.join()
    p2.join()
    p3.join()
    p4.join()
    p5.join()