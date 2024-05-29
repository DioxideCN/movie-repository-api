import type { Metadata } from "next";
import { AntdRegistry } from '@ant-design/nextjs-registry';
import "@/scss/globals.scss";
import React from "react";

export const metadata: Metadata = {
  title: "Movie Repository",
  description: "Movie repository frontend.",
  icons: {
    icon: {
      url: "/favicon.ico",
      href: "/favicon.ico",
    }
  },
  authors: {
    url: "https://dioxide-cn.ink",
    name: "DioxideCN",
  },
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="zh-cn">
      <body>
        <AntdRegistry>{children}</AntdRegistry>
      </body>
    </html>
  )
}
