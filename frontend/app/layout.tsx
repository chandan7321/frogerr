import type { Metadata } from 'next';
import './styles.css';

export const metadata: Metadata = { title: 'FORGER Control Plane', description: 'Distributed object storage demo' };

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>): React.ReactNode {
  return <html lang="en"><body>{children}</body></html>;
}
