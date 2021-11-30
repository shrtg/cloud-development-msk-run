process.stdin.on('data', (chunk) => {
    const inputString = chunk.toString().replace(/\n$/, '');
    const reversed = inputString.split('').reverse().join('');

    console.log(reversed + '\n');
});

