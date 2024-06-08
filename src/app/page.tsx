'use client';

import '@/scss/page.module.scss';
import {useEffect, useState} from "react";
import {ConfigProvider, Modal} from "antd";

export default function Home() {
  const [movies, setMovies]: [MovieType[], any] = useState<MovieType[]>([]);
  const [open, setOpen]: [boolean, any] = useState(false);
  const [currMovie, setCurrMovie]: [MovieType | undefined, any] = useState<MovieType>();
  const [selectPlatformIdx, setPlatformIdx]: [number, any] = useState<number>(0);

  useEffect((): void => {
    const fetchMovies = async (): Promise<void> => {
      try {
        const response = await fetch('http://localhost:8000/fetch/movies?page=1&pagesize=42');
        if (response.status >= 200 && response.status < 300) {
          const data = await response.json();
          setMovies(data.data);
        } else {
          console.error('Failed to fetch movies:', response.statusText);
        }
      } catch (error) {
        console.error('Error fetching movies:', error);
      }
    };
    fetchMovies().then();
  }, []);

  const calculateAverageScore = (movies: PlatformDetailType[]): number | string => {
    const scores: number[] = movies
      .filter((detail: PlatformDetailType) => detail.score !== undefined && detail.score > 0.0)
      .map((detail: PlatformDetailType) => {
        if (detail.score && detail.score > 10.0) {
          return detail.score / 10;
        }
        return detail.score as number;
      });
    if (scores.length < 1) return '暂无评分';
    const total: number = scores.reduce((sum: number, score: number) => sum + score, 0.0);
    const average: string = (total / scores.length).toString();
    return parseFloat(average).toFixed(1);
  };

  const switchShowMovie = (movie: MovieType): void => {
    setCurrMovie(movie)
    setOpen(true)
    setPlatformIdx(0)
  }

  return (
    <main className="w-full h-screen">
      <div id="container" className="flex flex-wrap">
        {movies.map((movie: MovieType, index: number) => (
            <div key={index} className="w-1/2 sm:w-1/3 md:w-1/4 lg:w-1/5 xl:w-1/7 p-2">
              <div className="relative cursor-pointer rounded-md" onClick={() => switchShowMovie(movie)}>
                <img
                    src={`/proxy?url=${encodeURIComponent(movie.platform_detail[0].cover_url)}`}
                    alt={movie.fixed_title}
                    className="w-full h-auto rounded-md"
                />
                <div className="absolute bottom-0 right-0 text-score">{calculateAverageScore(movie.platform_detail)}</div>
              </div>
              <div className="text-title" onClick={() => switchShowMovie(movie)}>{movie.fixed_title}</div>
              <div className="text-subtitle">共 {movie.platform_detail.length} 条平台记录</div>
            </div>
        ))}
        <ConfigProvider
          theme={{
            token: {
              colorText: '#fff',
              colorIcon: '#fff',
            },
            components: {
              Modal: {
                contentBg: "transparent",
                headerBg: "transparent",
                titleColor: "#fff",
              }
            }
          }}
        >
          <Modal
              title={currMovie?.fixed_title}
              centered
              open={open}
              onOk={() => setOpen(false)}
              onCancel={() => setOpen(false)}
              footer={(): null => null}
              closeIcon={null}
              width={850}
              classNames={{
                content: 'movie-modal_content',
                header: 'movie-modal_not-display'
              }}
          >
            <div className="movie-modal_area flex">
              {currMovie?.platform_detail.map((detail: PlatformDetailType, index: number) => (
                  <div className={`movie-modal_area--selector rounded-md mr-3 ${currMovie?.platform_detail[selectPlatformIdx].source === detail.source ? 'active' : 'cursor-pointer'}`} key={index} onClick={() => {
                    if (index !== selectPlatformIdx)
                      setPlatformIdx(index)
                  }}>
                    {detail.source === 'iqiyi' ? '爱奇艺' : detail.source === 'tencent' ? '腾讯视频' : detail.source === 'mgtv' ? '芒果TV' : detail.source === 'bilibili' ? 'B站' : '优酷'}
                  </div>
              ))}
            </div>
            <div className="movie-modal_body flex rounded-md p-3">
              <div className="movie-modal_body--left flex-none mr-3">
                <img className="rounded-md w-64"
                     src={`/proxy?url=${encodeURIComponent(currMovie?.platform_detail[selectPlatformIdx].cover_url || '')}`} alt="cover" />
              </div>
              <div className="movie-modal_body--right pl-0 flex-1">
                <h2>{currMovie?.fixed_title}</h2>
                <div>
                  {currMovie?.platform_detail[selectPlatformIdx] && currMovie?.platform_detail[selectPlatformIdx]?.actors.length > 0 ? (
                      <div className="intro mt-3"><span>演员：</span>{currMovie?.platform_detail[selectPlatformIdx]?.actors.join(' / ')}</div>
                  ) : null}
                </div>
                <div className="intro mt-3">
                  <span>简介：</span>{currMovie?.platform_detail[selectPlatformIdx].description}</div>
              </div>
            </div>
          </Modal>
        </ConfigProvider>
      </div>
    </main>
  );
}
