"use client";

import { AntdRegistry } from '@ant-design/nextjs-registry';
import { usePathname } from 'next/navigation';
import { useEffect } from 'react';

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  const pathname = usePathname();
  const type = pathname.split('/').pop();

  useEffect(() => {
    document.title = `Momentum | ${type === 'login' ? '登录' : '注册'}`;
  }, [ type ])

  return (
    <html lang="zh-cn">
      <body>
        <AntdRegistry>{children}</AntdRegistry>
      </body>
    </html>
  )
}
