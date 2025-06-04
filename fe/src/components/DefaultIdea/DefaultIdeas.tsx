import DefaultIdea from "./DefaultIdea";

const defaultIdeas = [
  {
    idea: "Chương trình học",
    moreContext: "Chương trình học ngành khoa học máy tính",
  },
  {
    idea: "Môn học",
    moreContext:
      "Kỳ này mở những môn nào",
  },
  { idea: "Lớp hành chính", moreContext: "Lớp hành chính của tôi là gì" },
  {
    idea: "Lớp học phần",
    moreContext: "Lớp học phần MAT1041 bao nhiêu tín chỉ",
  },
];

export default function DefaultIdeas({ visible = true }) {
  return (
    <div className={`row1 ${visible ? "block" : "hidden"}`}>
      <DefaultIdea ideas={defaultIdeas.slice(0, 2)} />
      <DefaultIdea
        ideas={defaultIdeas.slice(2, 4)}
        myclassNames="hidden md:visible"
      />
    </div>
  );
}
