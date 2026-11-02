"""Capture actual benchmark terminal output and render a timed video replay."""
import argparse
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import re
import subprocess
import sys
import textwrap
import time


def visible_text(events, elapsed):
    return ''.join(event[2] for event in events if event[0] <= elapsed and event[1] == 'o')


def render(directory, *, speed=8, fps=4):
    import imageio_ffmpeg
    import matplotlib
    from PIL import Image, ImageDraw, ImageFont
    lines = (directory / 'session.cast').read_text(encoding='utf-8').splitlines()
    header, events = json.loads(lines[0]), [json.loads(line) for line in lines[1:]]
    duration = events[-1][0] if events else 0
    font = ImageFont.truetype(str(Path(matplotlib.get_data_path()) / 'fonts/ttf/DejaVuSansMono.ttf'), 16)
    ffmpeg = imageio_ffmpeg.get_ffmpeg_exe()
    video = directory / 'failover.mp4'
    command = [ffmpeg, '-hide_banner', '-loglevel', 'error', '-n', '-f', 'rawvideo', '-vcodec', 'rawvideo',
               '-s', '1280x720', '-pix_fmt', 'rgb24', '-r', str(fps), '-i', '-', '-an',
               '-c:v', 'libx264', '-preset', 'fast', '-crf', '24', '-pix_fmt', 'yuv420p',
               '-movflags', '+faststart', str(video)]
    encoder = subprocess.Popen(command, stdin=subprocess.PIPE)
    try:
        for frame in range(math.ceil((duration / speed + 3) * fps)):
            elapsed = min(duration, frame / fps * speed)
            content = re.sub(r'\x1b\[[0-9;]*[A-Za-z]', '', visible_text(events, elapsed)).replace('\r', '')
            wrapped = [part for line in content.splitlines() for part in textwrap.wrap(line, width=124) or ['']]
            canvas = Image.new('RGB', (1280, 720), '#111827')
            draw = ImageDraw.Draw(canvas)
            draw.text((24, 16), f'E4b | recorded terminal replay {speed}x | source elapsed {elapsed:.1f}s',
                      fill='#7dd3fc', font=font)
            draw.text((24, 42), header['title'], fill='#94a3b8', font=font)
            for index, line in enumerate(wrapped[-28:]):
                draw.text((24, 78 + index * 22), line, fill='#86efac' if 'PASS' in line else '#e2e8f0', font=font)
            encoder.stdin.write(canvas.tobytes())
        canvas.save(directory / 'poster.png')
    finally:
        encoder.stdin.close()
    if encoder.wait() != 0:
        raise RuntimeError('Terminal video encoder failed')
    from src.monitoring.e4b_failover import save_json
    save_json(directory / 'video-metadata.json', {'kind': 'Actual stdout terminal capture replay',
        'source': 'session.cast', 'speed': speed, 'fps': fps, 'source_duration_seconds': duration,
        'video_duration_seconds': duration/speed + 3, 'video': video.name,
        'recording_started_at': datetime.fromtimestamp(header['timestamp'], timezone.utc).isoformat()})


def capture(directory, rows):
    # Bootstrap may have created the directory; exclusive artifacts prevent reuse.
    with (directory / 'session.cast').open('x', encoding='utf-8') as cast, \
            (directory / 'terminal.log').open('x', encoding='utf-8') as output:
        cast.write(json.dumps({'version': 2, 'width': 160, 'height': 32, 'timestamp': int(time.time()),
                               'title': f'{directory.name} | real Kafka -> Delta SIGKILL / checkpoint resume'}) + '\n')
        cast.flush()
        began = time.monotonic()
        command = [sys.executable, '-u', '-m', 'src.monitoring.e4b_failover',
                   '--directory', str(directory), '--rows', str(rows)]
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                   text=True, bufsize=1)
        for line in process.stdout:
            output.write(line)
            output.flush()
            cast.write(json.dumps([time.monotonic()-began, 'o', line]) + '\n')
            cast.flush()
            print(line, end='', flush=True)
        code = process.wait()
    render(directory)
    return code


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--rows', type=int, default=20000)
    parser.add_argument('--render-only', action='store_true')
    args = parser.parse_args()
    if args.render_only:
        render(args.directory)
    else:
        raise SystemExit(capture(args.directory, args.rows))


if __name__ == '__main__':
    main()
