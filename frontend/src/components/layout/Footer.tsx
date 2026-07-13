import styles from './Footer.module.css';

export default function Footer () {
  return (
    <div>
      {/* line break */}
      <hr className = {styles.greyLine}></hr>

      {/* footer */}
      <footer className = {styles.footer}>
        <div className = {styles.container}>
          <p className = {styles.text}>
            © 2026 EagleView | Built by Scott Lam | <a href="https://github.com/scottlam01" target="_blank">GitHub</a>
          </p>
        </div>
      </footer>
    </div>
  )
}