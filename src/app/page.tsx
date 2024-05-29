'use client';

import '@/scss/page.module.scss';
import {useEffect, useState} from "react";

export default function Home() {
  const [movies, setMovies]: [MovieType[], any] = useState<MovieType[]>([]);

  useEffect(() => {
    const fetchMovies = async () => {
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

  return (
    <main className="w-full h-screen">
      <div id="container" className="flex flex-wrap">
        {movies.map((movie: MovieType, index: number) => (
            <div key={index} className="w-1/2 sm:w-1/3 md:w-1/4 lg:w-1/5 xl:w-1/7 p-2">
              <img src={movie.platform_detail[0].cover_url} alt={movie.fixed_title} className="w-full h-auto rounded-md"/>
              <div className="text-title">{movie.fixed_title}</div>
              <div className="text-subtitle">共 {movie.platform_detail.length} 条平台记录</div>
            </div>
        ))}
      </div>
    </main>
  );
}
