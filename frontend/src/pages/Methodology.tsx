import Header from '../components/layout/Header';
import styles from './Methodology.module.css';
import Card from '../components/ui/Card';
import Footer from '../components/layout/Footer';
import TableOfContents from '../components/methodology/TableOfContents';
import methodology from "./MethodologyContent.md?raw";

import ReactMarkdown from "react-markdown";
import remarkMath from "remark-math";
import rehypeKatex from "rehype-katex";
import "katex/dist/katex.min.css";
import rehypeSlug from "rehype-slug";

export function Methodology() {

  // handles scrolling to proper section when TOC section clicked
  const scrollToSection = (id: string) => {

    const element = document.getElementById(id);

    element?.scrollIntoView({
      behavior: "smooth",
      block: "start",
    });
  };

  return (
    <div className={styles.page}>

      <Header />

      <div className={styles.methodology}>

        <Card className={styles.methodologyCard}>

          <h1>EagleView Methodology</h1>

          <div className={styles.methodologyContent}>

            <TableOfContents onSectionClick={scrollToSection} />

            <div className={styles.divider}></div>

            <article className={styles.content}>
              <ReactMarkdown
                remarkPlugins={[remarkMath]}
                rehypePlugins={[rehypeSlug, rehypeKatex]}
              >
                {methodology}
              </ReactMarkdown>
            </article>

          </div>

        </Card>

      </div>

      <Footer />

    </div>
    
  );
}

export default Methodology;