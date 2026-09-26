import { NextRequest, NextResponse } from 'next/server';

export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

async function proxy(request: NextRequest, context: { params: Promise<{ path: string[] }> }): Promise<NextResponse> {
	const { path } = await context.params;
  const base = process.env.FORGER_BACKEND_URL?.replace(/\/$/, '');
  if (!base) return NextResponse.json({ error: 'FORGER_BACKEND_URL is not configured' }, { status: 503 });
	const target = `${base}/${path.join('/')}${request.nextUrl.search}`;
  const headers = new Headers(request.headers);
  headers.delete('host');
  headers.delete('connection');
	if (path[0] === 'v1' && path[1] === 'admin' && process.env.FORGER_ADMIN_TOKEN) {
    headers.set('X-Forger-Admin-Token', process.env.FORGER_ADMIN_TOKEN);
  }
  try {
    const response = await fetch(target, { method: request.method, headers, body: ['GET', 'HEAD'].includes(request.method) ? undefined : request.body, // @ts-expect-error required by Node fetch for streamed request bodies
      duplex: 'half', cache: 'no-store' });
    const output = new Headers(response.headers);
    output.delete('transfer-encoding');
    output.delete('connection');
    return new NextResponse(response.body, { status: response.status, headers: output });
  } catch {
    return NextResponse.json({ error: 'Coordinator is unreachable' }, { status: 502 });
  }
}
export { proxy as GET, proxy as POST, proxy as PUT, proxy as DELETE, proxy as HEAD };
