import type { Metadata } from "next";
import { AntdRegistry } from '@ant-design/nextjs-registry';
import "@/scss/globals.scss";

export const metadata: Metadata = {
  title: "Momentum City Brain",
  description: "Momentum city brain.",
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
